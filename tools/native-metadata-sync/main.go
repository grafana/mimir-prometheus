// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

type options struct {
	work, input, out, envelope, normalization, check, diagnostics, repo string
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:])
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "native metadata sync:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("expected prepare, normalize, sign, validate, repair, seal, publish, or bootstrap-check")
	}
	command := args[0]
	var o options
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.StringVar(&o.work, "work", "", "fresh scratch repository")
	f.StringVar(&o.input, "input", "", "input artifact directory")
	f.StringVar(&o.out, "out", "", "output artifact directory")
	f.StringVar(&o.envelope, "envelope", "", "expected signed envelope commit")
	f.StringVar(&o.normalization, "normalization", "", "normalization patch")
	f.StringVar(&o.check, "check", "", "validation check")
	f.StringVar(&o.diagnostics, "diagnostics", "", "diagnostic file or directory")
	f.StringVar(&o.repo, "repo", "", "local repository for offline bootstrap verification")
	if err := f.Parse(args[1:]); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if command == "bootstrap-check" {
		return bootstrapCheck(ctx, o.repo, o.work)
	}
	if !slices.Contains([]string{"prepare", "normalize", "sign", "validate", "repair", "seal", "publish"}, command) {
		return errors.New("unknown command")
	}
	if os.Getenv("GITHUB_REPOSITORY") != repository || os.Getenv("GITHUB_REF") != "refs/heads/main" || !oidPattern.MatchString(os.Getenv("GITHUB_SHA")) {
		return errors.New("automation must run from grafana/mimir-prometheus main")
	}
	if command != "publish" && os.Getenv("GITHUB_RUN_ATTEMPT") != "1" {
		return errors.New("reruns are disabled; dispatch a new run")
	}
	if o.work == "" {
		return errors.New("--work is required")
	}
	if _, err := os.Stat(o.work); !errors.Is(err, os.ErrNotExist) {
		return errors.New("--work must not exist")
	}
	if o.out == "" && command != "validate" {
		return errors.New("--out is required")
	}
	if command == "publish" {
		for _, name := range []string{"GH_TOKEN", "GITHUB_TOKEN"} {
			if os.Getenv(name) == "" {
				return fmt.Errorf("%s is required for %s", name, command)
			}
		}
	}
	name, email := os.Getenv("SYNC_BOT_NAME"), os.Getenv("SYNC_BOT_EMAIL")
	if name == "" || email == "" || strings.ContainsAny(name+email, "\r\n") {
		return errors.New("bot identity variables are required")
	}
	allowed, key, cleanup, err := signingFiles(ctx, email, command == "sign" || command == "seal")
	if err != nil {
		return err
	}
	defer cleanup()
	client := &http.Client{Timeout: 10 * time.Minute, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	validation := github{client: client, endpoint: "https://api.github.com", token: os.Getenv("GITHUB_TOKEN")}
	r := claude{client: client, endpoint: "https://api.anthropic.com/v1/messages", key: os.Getenv("ANTHROPIC_API_KEY"), ledgerPath: filepath.Join(o.out, "ledger.json")}
	if command == "prepare" {
		g, err := initRepo(ctx, o.work, name, email)
		if err != nil {
			return err
		}
		g.allowedSigners = allowed
		return g.prepare(ctx, o.out, r)
	}
	if command != "sign" && command != "normalize" && !oidPattern.MatchString(o.envelope) {
		return errors.New("expected signed envelope is required")
	}
	g, m, err := importPayload(ctx, o.work, o.input, originURL, name, email, allowed, o.envelope)
	if err != nil {
		return err
	}
	runID, err := envInt("GITHUB_RUN_ID")
	if err != nil {
		return err
	}
	if m.Control != os.Getenv("GITHUB_SHA") || m.Ledger.RunID != runID || m.Ledger.Attempt != 1 {
		return errors.New("artifact belongs to another workflow run")
	}
	switch command {
	case "normalize":
		return g.normalize(ctx, o.out)
	case "sign":
		g.key = key
		if err = g.applyNormalization(ctx, &m, o.normalization); err != nil {
			return err
		}
		if err = g.formatGo(ctx, &m); err != nil {
			return err
		}
		envelope, e := g.signCandidate(ctx, &m)
		if e != nil {
			return e
		}
		if e = g.export(ctx, o.out, m, envelope); e != nil {
			return e
		}
		if e = githubOutput("candidate", m.Candidate); e != nil {
			return e
		}
		return githubOutput("envelope", envelope)
	case "validate":
		return g.validate(ctx, m, o.check, o.diagnostics)
	case "repair":
		if err = g.repair(ctx, &m, r, o.diagnostics); err != nil {
			return err
		}
		// Remove the previous signed envelope before exporting a changed candidate.
		if _, err = g.git(ctx, "update-ref", "-d", "refs/heads/envelope"); err != nil {
			return err
		}
		return g.export(ctx, o.out, m, "")
	case "seal":
		g.key = key
		receipt, e := g.seal(ctx, &m, validation)
		if e != nil {
			return e
		}
		if e = g.export(ctx, o.out, m, receipt); e != nil {
			return e
		}
		if e = g.report(ctx, o.out, m, "validated"); e != nil {
			return e
		}
		return githubOutput("envelope", receipt)
	case "publish":
		g.token = os.Getenv("GH_TOKEN")
		status, publishErr := g.publish(ctx, originURL, o.envelope, m, validation)
		if status == "" {
			return publishErr
		}
		fmt.Printf("Native metadata sync publication: %s\n", status)
		outputErr := githubOutput("publication", status)
		reportErr := g.report(ctx, o.out, m, status)
		if reportErr != nil || outputErr != nil {
			return errors.Join(publishErr, fmt.Errorf("publication %s; reporting failed: %w", status, errors.Join(outputErr, reportErr)))
		}
		return publishErr
	}
	return errors.New("unhandled command")
}

func signingFiles(ctx context.Context, email string, private bool) (allowed, key string, cleanup func(), err error) {
	dir, err := os.MkdirTemp("", "native-metadata-signing-")
	if err != nil {
		return "", "", func() {}, err
	}
	cleanup = func() { _ = os.RemoveAll(dir) }
	defer func() {
		if err != nil {
			cleanup()
		}
	}()
	public := strings.TrimSpace(os.Getenv("SYNC_SIGNING_PUBLIC_KEY"))
	fields := strings.Fields(public)
	if len(fields) < 2 || fields[0] != "ssh-ed25519" || strings.ContainsAny(public, "\r\n") {
		return "", "", cleanup, errors.New("an Ed25519 signing public key is required")
	}
	public = strings.Join(fields[:2], " ")
	allowed = filepath.Join(dir, "allowed_signers")
	if err = os.WriteFile(allowed, []byte(email+" "+public+"\n"), 0o600); err != nil {
		return
	}
	if !private {
		return
	}
	secret := os.Getenv("SYNC_SIGNING_PRIVATE_KEY")
	if secret == "" {
		return "", "", cleanup, errors.New("signing private key is required")
	}
	key = filepath.Join(dir, "key")
	if err = os.WriteFile(key, []byte(strings.TrimSpace(secret)+"\n"), 0o600); err != nil {
		return
	}
	cmd := exec.CommandContext(ctx, "ssh-keygen", "-y", "-P", "", "-f", key)
	cmd.Env = commandEnv()
	var derived []byte
	derived, err = cmd.Output()
	if err != nil {
		return
	}
	derivedFields := strings.Fields(string(derived))
	if len(derivedFields) < 2 || strings.Join(derivedFields[:2], " ") != public {
		err = errors.New("private signing key does not match the configured public key")
	}
	return
}

func (g gitRepo) prepare(ctx context.Context, out string, r resolver) error {
	previous, err := bootstrap()
	if err != nil {
		return err
	}
	target, err := g.fetch(ctx, originURL, "refs/heads/"+topic, "refs/remotes/target")
	if err != nil {
		return err
	}
	main, err := g.fetch(ctx, originURL, "refs/heads/main", "refs/remotes/main")
	if err != nil {
		return err
	}
	source, err := g.fetch(ctx, sourceURL, "refs/heads/"+topic, "refs/remotes/source")
	if err != nil {
		return err
	}
	official, err := g.fetch(ctx, upstreamURL, "refs/heads/main", "refs/remotes/official")
	if err != nil {
		return err
	}
	state, err := g.remoteRef(ctx, originURL, stateRef)
	if err != nil {
		return err
	}
	if state != "" {
		if _, err = g.fetch(ctx, originURL, stateRef, "refs/remotes/state"); err != nil {
			return err
		}
		previous, err = g.loadManifest(ctx, state, true)
		if err != nil {
			return err
		}
		if err = g.verifyReceipt(ctx, state, previous); err != nil {
			return err
		}
		applied, e := g.publicationReceipt(ctx, originURL, state, previous)
		if e != nil {
			return e
		}
		if !applied {
			return errors.New("applied checkpoint has no publication receipt")
		}
	} else {
		if _, err = g.fetch(ctx, sourceURL, previous.SourceHead, "refs/remotes/bootstrap"); err != nil {
			return fmt.Errorf("bootstrap source history must remain fetchable until first publication: %w", err)
		}
	}
	if _, err = g.linear(ctx, previous.Candidate, target); err != nil {
		return fmt.Errorf("target no longer descends linearly from the checkpoint: %w", err)
	}
	control := os.Getenv("GITHUB_SHA")
	ok, err := g.ancestor(ctx, control, main)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("control checkout is not an ancestor of current main")
	}
	policy, err := g.policyDigest(ctx, main)
	if err != nil {
		return err
	}
	controlPolicy, err := g.policyDigest(ctx, control)
	if err != nil {
		return err
	}
	if policy != controlPolicy {
		return errors.New("automation changed since dispatch; start a new run")
	}
	if previous.Main == main && previous.SourceHead == source {
		return githubOutput("noop", "true")
	}
	base := previous.SourceBase
	ff, err := g.ancestor(ctx, previous.SourceHead, source)
	if err != nil {
		return err
	}
	if !ff {
		base, err = g.git(ctx, "merge-base", "--all", source, official)
		if err != nil {
			return err
		}
		if !oidPattern.MatchString(base) {
			return errors.New("rewritten source has no unique official-main merge base")
		}
	}
	runID, err := envInt("GITHUB_RUN_ID")
	if err != nil {
		return err
	}
	m := manifest{
		Version: schemaVersion, Repository: repository, TargetRef: "refs/heads/" + topic,
		Target: target, PreviousState: state, Main: main, Control: control, Policy: policy,
		SourceBase: base, SourceHead: source, PreviousSourceHead: previous.SourceHead, OfficialMain: official,
		Created: time.Now().UTC().Format(time.RFC3339), Ledger: ledger{RunID: runID, Attempt: 1},
	}
	gap, err := g.git(ctx, "rev-list", "--reverse", main+".."+base)
	if err != nil {
		return err
	}
	if gap != "" {
		m.BaseGap = strings.Split(gap, "\n")
	}
	if err = g.replay(ctx, previous, &m, r); err != nil {
		return err
	}
	if err = g.formatGo(ctx, &m); err != nil {
		return err
	}
	if err = g.export(ctx, out, m, ""); err != nil {
		return err
	}
	return githubOutput("noop", "false")
}

func bootstrapCheck(ctx context.Context, repo, work string) error {
	if repo == "" || work == "" {
		return errors.New("--repo and a fresh --work directory are required")
	}
	if _, err := os.Stat(work); !errors.Is(err, os.ErrNotExist) {
		return errors.New("--work must not exist")
	}
	local, err := filepath.Abs(repo)
	if err != nil {
		return err
	}
	b, err := bootstrap()
	if err != nil {
		return err
	}
	g, err := initRepo(ctx, work, "Bootstrap verifier", "bootstrap@example.invalid")
	if err != nil {
		return err
	}
	for i, oid := range []string{b.Target, b.SourceHead} {
		if _, err = g.fetch(ctx, local, oid, fmt.Sprintf("refs/remotes/bootstrap-%d", i)); err != nil {
			return err
		}
	}
	m := b
	if err = g.replay(ctx, b, &m, nil); err != nil {
		return err
	}
	before, err := g.git(ctx, "rev-parse", b.Target+"^{tree}")
	if err != nil {
		return err
	}
	after, err := g.git(ctx, "rev-parse", m.Candidate+"^{tree}")
	if err != nil {
		return err
	}
	if before != after {
		return errors.New("bootstrap replay did not reproduce the existing feature tree")
	}
	fmt.Printf("Bootstrap verified: %d source projections, %d overlays; tree %s\n", len(m.Mapping), len(m.Overlays), after)
	return nil
}

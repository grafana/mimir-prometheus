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
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

type gitRepo struct {
	dir            string
	name           string
	email          string
	key            string
	token          string
	allowedSigners string
}

func commandEnv(extra ...string) []string {
	var env []string
	for _, v := range os.Environ() {
		name, _, _ := strings.Cut(v, "=")
		if strings.HasPrefix(name, "GIT_") || name == "ANTHROPIC_API_KEY" || name == "SYNC_SIGNING_PRIVATE_KEY" || name == "GH_TOKEN" || name == "GITHUB_TOKEN" {
			continue
		}
		env = append(env, v)
	}
	return append(env, extra...)
}

func (g gitRepo) command(ctx context.Context, input []byte, extra []string, args ...string) ([]byte, error) {
	options := []string{
		"-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
		"-c", "core.attributesFile=/dev/null", "-c", "core.autocrlf=false",
		"-c", "commit.gpgsign=false", "-c", "protocol.file.allow=always",
		"-c", "user.name=" + g.name, "-c", "user.email=" + g.email,
		"-c", "gpg.format=ssh", "-c", "gpg.ssh.program=ssh-keygen",
	}
	if g.key != "" {
		options = append(options, "-c", "user.signingkey="+g.key)
	}
	if g.allowedSigners != "" {
		options = append(options, "-c", "gpg.ssh.allowedSignersFile="+g.allowedSigners)
	}
	if g.token != "" {
		extra = append(extra, "GIT_CONFIG_COUNT=1", "GIT_CONFIG_KEY_0=http.https://github.com/.extraheader", "GIT_CONFIG_VALUE_0=AUTHORIZATION: basic "+base64.StdEncoding.EncodeToString([]byte("x-access-token:"+g.token)))
	}
	cmd := exec.CommandContext(ctx, "git", append(options, args...)...)
	cmd.Dir = g.dir
	cmd.Env = commandEnv(append([]string{
		"GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_NO_REPLACE_OBJECTS=1",
		"GIT_TERMINAL_PROMPT=0", "GIT_LFS_SKIP_SMUDGE=1", "LC_ALL=C",
	}, extra...)...)
	cmd.Stdin = bytes.NewReader(input)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	b, err := cmd.Output()
	if err != nil {
		return b, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, stderr.String())
	}
	return b, nil
}

func (g gitRepo) git(ctx context.Context, args ...string) (string, error) {
	b, err := g.command(ctx, nil, nil, args...)
	return strings.TrimSpace(string(b)), err
}

func initRepo(ctx context.Context, dir, name, email string) (gitRepo, error) {
	g := gitRepo{dir: dir, name: name, email: email}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return g, err
	}
	_, err := g.git(ctx, "init", "--quiet", "--initial-branch=transport")
	return g, err
}

func (g gitRepo) fetch(ctx context.Context, remote, ref, dest string) (string, error) {
	if _, err := g.git(ctx, "fetch", "--quiet", "--no-tags", "--no-recurse-submodules", remote, "+"+ref+":"+dest); err != nil {
		return "", err
	}
	return g.git(ctx, "rev-parse", "--verify", dest+"^{commit}")
}

func (g gitRepo) remoteRef(ctx context.Context, remote, ref string) (string, error) {
	s, err := g.git(ctx, "ls-remote", "--refs", remote, ref)
	if err != nil {
		return "", err
	}
	if s == "" {
		return "", nil
	}
	fields := strings.Fields(s)
	if len(fields) != 2 || fields[1] != ref || !oidPattern.MatchString(fields[0]) {
		return "", errors.New("ambiguous remote ref")
	}
	return fields[0], nil
}

func (g gitRepo) ancestor(ctx context.Context, a, b string) (bool, error) {
	_, err := g.git(ctx, "merge-base", "--is-ancestor", a, b)
	if err == nil {
		return true, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return false, nil
	}
	return false, err
}

func (g gitRepo) linear(ctx context.Context, base, tip string) ([]string, error) {
	ok, err := g.ancestor(ctx, base, tip)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("base is not an ancestor of the tip")
	}
	merges, err := g.git(ctx, "rev-list", "--min-parents=2", base+".."+tip)
	if err != nil {
		return nil, err
	}
	if merges != "" {
		return nil, errors.New("nonlinear feature or local history requires manual integration")
	}
	s, err := g.git(ctx, "rev-list", "--reverse", base+".."+tip)
	if err != nil {
		return nil, err
	}
	if s == "" {
		return nil, nil
	}
	return strings.Split(s, "\n"), nil
}

func (g gitRepo) delta(ctx context.Context, oid string) (string, error) {
	b, err := g.command(ctx, nil, nil, "diff-tree", "--no-commit-id", "--raw", "-r", "-z", "--no-renames", "--no-abbrev", oid)
	return digest(b), err
}

func (g gitRepo) changed(ctx context.Context, a, b string) ([]string, error) {
	data, err := g.command(ctx, nil, nil, "diff", "--no-ext-diff", "--no-textconv", "--name-only", "-z", a, b)
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}
	return strings.Split(strings.TrimSuffix(string(data), "\x00"), "\x00"), nil
}

func (g gitRepo) checkPolicy(ctx context.Context, main, candidate string) error {
	paths, err := g.changed(ctx, main, candidate)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if protected(path) {
			return fmt.Errorf("candidate changes trusted automation path %q; integrate it through main first", path)
		}
	}
	return nil
}

func (g gitRepo) policyDigest(ctx context.Context, ref string) (string, error) {
	args := append([]string{"ls-tree", "-r", "-z", ref, "--"}, protectedPaths...)
	b, err := g.command(ctx, nil, nil, args...)
	return digest(b), err
}

func (g gitRepo) verify(ctx context.Context, oid string) error {
	if g.allowedSigners == "" {
		return errors.New("a trusted signing public key is required")
	}
	_, err := g.git(ctx, "verify-commit", oid)
	return err
}

func (g gitRepo) loadManifest(ctx context.Context, oid string, signed bool) (manifest, error) {
	var m manifest
	if !oidPattern.MatchString(oid) {
		return m, errors.New("invalid manifest commit")
	}
	if signed {
		if err := g.verify(ctx, oid); err != nil {
			return m, err
		}
	}
	files, err := g.git(ctx, "ls-tree", "-r", "--name-only", oid)
	if err != nil {
		return m, err
	}
	if files != "manifest.json" {
		return m, errors.New("metadata commit must contain only manifest.json")
	}
	b, err := g.command(ctx, nil, nil, "show", oid+":manifest.json")
	if err != nil {
		return m, err
	}
	if err = decodeJSON(b, &m); err != nil {
		return m, err
	}
	return m, m.valid()
}

func (g gitRepo) commitTree(ctx context.Context, tree string, parents []string, message string, author []string, date string, signed bool) (string, error) {
	if strings.ContainsAny(g.name+g.email, "\r\n") || g.name == "" || g.email == "" {
		return "", errors.New("invalid bot identity")
	}
	signoff := "Signed-off-by: " + g.name + " <" + g.email + ">"
	if !slices.Contains(strings.Split(message, "\n"), signoff) {
		message = strings.TrimRight(message, "\n") + "\n\n" + signoff + "\n"
	}
	args := []string{"commit-tree", tree}
	for _, p := range parents {
		args = append(args, "-p", p)
	}
	if signed {
		if g.key == "" {
			return "", errors.New("signing key is required")
		}
		args = append(args, "-S")
	}
	env := append([]string{"GIT_COMMITTER_DATE=" + date}, author...)
	b, err := g.command(ctx, []byte(message), env, args...)
	return strings.TrimSpace(string(b)), err
}

func (g gitRepo) copyCommit(ctx context.Context, original, parent, date string, signed bool) (string, error) {
	tree, err := g.git(ctx, "rev-parse", original+"^{tree}")
	if err != nil {
		return "", err
	}
	b, err := g.command(ctx, nil, nil, "show", "-s", "--format=%an%x00%ae%x00%aI%x00%B", original)
	if err != nil {
		return "", err
	}
	fields := strings.SplitN(string(b), "\x00", 4)
	if len(fields) != 4 {
		return "", errors.New("invalid original commit")
	}
	env := []string{"GIT_AUTHOR_NAME=" + fields[0], "GIT_AUTHOR_EMAIL=" + fields[1], "GIT_AUTHOR_DATE=" + fields[2]}
	return g.commitTree(ctx, tree, []string{parent}, fields[3], env, date, signed)
}

func (g gitRepo) metadata(ctx context.Context, m manifest, parents []string) (string, error) {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	blob, err := g.command(ctx, append(b, '\n'), nil, "hash-object", "-w", "--stdin")
	if err != nil {
		return "", err
	}
	tree, err := g.command(ctx, []byte("100644 blob "+strings.TrimSpace(string(blob))+"\tmanifest.json\n"), nil, "mktree")
	if err != nil {
		return "", err
	}
	return g.commitTree(ctx, strings.TrimSpace(string(tree)), parents, "ci: seal native metadata sync "+m.ID, nil, m.Created, true)
}

func (g gitRepo) export(ctx context.Context, dir string, m manifest, envelope string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if _, err := g.git(ctx, "update-ref", "refs/heads/candidate", m.Candidate); err != nil {
		return err
	}
	if envelope != "" {
		if _, err := g.git(ctx, "update-ref", "refs/heads/envelope", envelope); err != nil {
			return err
		}
	}
	path, err := filepath.Abs(filepath.Join(dir, "repository.bundle"))
	if err != nil {
		return err
	}
	// Bundle files are artifacts, never pushed as upstream branch tips.
	if _, err = g.git(ctx, "bundle", "create", path, "--all", "^"+m.Main); err != nil {
		return err
	}
	if err = writeJSON(filepath.Join(dir, "manifest.json"), m); err != nil {
		return err
	}
	return writeJSON(filepath.Join(dir, "transport.json"), struct {
		Envelope string `json:"envelope"`
	}{envelope})
}

func importPayload(ctx context.Context, dir, input, remote, name, email, allowedSigners string, expectedEnvelope string) (gitRepo, manifest, error) {
	var m manifest
	g, err := initRepo(ctx, dir, name, email)
	if err != nil {
		return g, m, err
	}
	g.allowedSigners = allowedSigners
	b, err := os.ReadFile(filepath.Join(input, "manifest.json"))
	if err != nil {
		return g, m, err
	}
	if err = decodeJSON(b, &m); err != nil {
		return g, m, err
	}
	if err = m.valid(); err != nil {
		return g, m, err
	}
	if _, err = g.fetch(ctx, remote, m.Main, "refs/remotes/main"); err != nil {
		return g, m, err
	}
	bundle, err := filepath.Abs(filepath.Join(input, "repository.bundle"))
	if err != nil {
		return g, m, err
	}
	if _, err = g.git(ctx, "bundle", "verify", bundle); err != nil {
		return g, m, err
	}
	heads, err := g.git(ctx, "bundle", "list-heads", bundle)
	if err != nil {
		return g, m, err
	}
	for line := range strings.SplitSeq(heads, "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 || !oidPattern.MatchString(fields[0]) || !allowedBundleRef(fields[1]) {
			return g, m, errors.New("unexpected bundle ref")
		}
	}
	if _, err = g.git(ctx, "fetch", "--quiet", "--no-tags", "--no-recurse-submodules", bundle, "+refs/heads/*:refs/heads/*", "+refs/remotes/*:refs/remotes/*"); err != nil {
		return g, m, err
	}
	if expectedEnvelope != "" {
		actual, e := g.git(ctx, "rev-parse", "refs/heads/envelope")
		if e != nil || actual != expectedEnvelope {
			return g, m, errors.New("unexpected envelope commit")
		}
		sealed, e := g.loadManifest(ctx, expectedEnvelope, true)
		if e != nil {
			return g, m, e
		}
		a, _ := json.Marshal(m)
		b, _ := json.Marshal(sealed)
		if !bytes.Equal(a, b) {
			return g, m, errors.New("transport manifest differs from signed manifest")
		}
	}
	if _, err = g.git(ctx, "checkout", "--quiet", "--detach", m.Candidate); err != nil {
		return g, m, err
	}
	return g, m, nil
}

func allowedBundleRef(ref string) bool {
	return ref == "HEAD" || ref == "refs/heads/candidate" || ref == "refs/heads/envelope" ||
		ref == "refs/remotes/main" || ref == "refs/remotes/source" || ref == "refs/remotes/official" ||
		ref == "refs/remotes/target" || ref == "refs/remotes/state" || ref == "refs/remotes/bootstrap"
}

func (g gitRepo) atomicPush(ctx context.Context, remote string, updates []refUpdate) error {
	args := []string{"push", "--atomic", "--porcelain"}
	seen := map[string]bool{}
	for _, u := range updates {
		if !oidPattern.MatchString(u.New) || u.Old != "" && !oidPattern.MatchString(u.Old) ||
			(!strings.HasPrefix(u.Ref, botPrefix) && u.Ref != "refs/heads/"+topic) || seen[u.Ref] {
			return errors.New("invalid atomic ref update")
		}
		seen[u.Ref] = true
		args = append(args, "--force-with-lease="+u.Ref+":"+u.Old)
	}
	args = append(args, remote)
	for _, u := range updates {
		args = append(args, u.New+":"+u.Ref)
	}
	_, err := g.git(ctx, args...)
	return err
}

type refUpdate struct{ Ref, Old, New string }

func githubOutput(name, value string) error {
	if strings.ContainsAny(name+value, "\r\n") {
		return errors.New("invalid workflow output")
	}
	path := os.Getenv("GITHUB_OUTPUT")
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = fmt.Fprintf(f, "%s=%s\n", name, value)
	return err
}

func envInt(name string) (int64, error) {
	n, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

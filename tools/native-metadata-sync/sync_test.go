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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

const testDate = "2026-09-18T00:00:00Z"

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func git(t *testing.T, g gitRepo, args ...string) string {
	t.Helper()
	result, err := g.git(t.Context(), args...)
	must(t, err)
	return result
}

func write(t *testing.T, root, name, content string) {
	t.Helper()
	must(t, os.MkdirAll(filepath.Dir(filepath.Join(root, name)), 0o700))
	must(t, os.WriteFile(filepath.Join(root, name), []byte(content), 0o644))
}

func commit(t *testing.T, g gitRepo, parent, message string, files map[string]string) string {
	t.Helper()
	if parent != "" {
		git(t, g, "checkout", "--quiet", "--detach", parent)
	}
	for name, content := range files {
		write(t, g.dir, name, content)
	}
	git(t, g, "add", "--all")
	tree := git(t, g, "write-tree")
	var parents []string
	if parent != "" {
		parents = []string{parent}
	}
	oid, err := g.commitTree(t.Context(), tree, parents, message, []string{"GIT_AUTHOR_NAME=Upstream author", "GIT_AUTHOR_EMAIL=author@example.invalid", "GIT_AUTHOR_DATE=" + testDate}, testDate, false)
	must(t, err)
	git(t, g, "checkout", "--quiet", "--detach", oid)
	return oid
}

func fixture(t *testing.T) (gitRepo, manifest) {
	t.Helper()
	g, err := initRepo(t.Context(), t.TempDir(), "Sync Bot", "bot@example.invalid")
	must(t, err)
	base := commit(t, g, "", "base", map[string]string{"a.txt": "base\n", "fork.txt": "base fork\n", ".github/workflows/ci.yml": "trusted\n"})
	main := commit(t, g, base, "fork main", map[string]string{"fork.txt": "fork main\n"})
	source := commit(t, g, base, "feature", map[string]string{"a.txt": "source\n"})
	local := commit(t, g, main, "feature projection", map[string]string{"a.txt": "source\n", "fork.txt": "fork adaptation\n"})
	overlay := commit(t, g, local, "local overlay", map[string]string{"overlay.txt": "local overlay\n"})
	delta, err := g.delta(t.Context(), source)
	must(t, err)
	policy, err := g.policyDigest(t.Context(), main)
	must(t, err)
	m := manifest{
		Version: schemaVersion, Repository: repository, TargetRef: "refs/heads/" + topic, Target: overlay, Main: main, Control: main, Policy: policy, SourceBase: base, SourceHead: source, Candidate: overlay,
		Mapping: []projection{{Source: source, Local: local, Delta: delta, Adapted: true}}, Overlays: []string{overlay}, Ledger: ledger{RunID: 42, Attempt: 1}, Created: testDate,
	}
	must(t, g.checkMapping(t.Context(), m))
	return g, m
}

func signing(t *testing.T, g *gitRepo) {
	t.Helper()
	key := filepath.Join(t.TempDir(), "key")
	cmd := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key)
	b, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("ssh-keygen: %v: %s", err, b)
	}
	pub, err := os.ReadFile(key + ".pub")
	must(t, err)
	g.key = key
	g.allowedSigners = filepath.Join(t.TempDir(), "allowed")
	must(t, os.WriteFile(g.allowedSigners, []byte(g.email+" "+string(pub)), 0o600))
}

type resolverFunc func(context.Context, resolveRequest, *ledger) (resolution, error)

func (f resolverFunc) resolve(ctx context.Context, r resolveRequest, l *ledger) (resolution, error) {
	return f(ctx, r, l)
}

func TestReplayRetainsForkProjectionAndHumanOverlay(t *testing.T) {
	g, previous := fixture(t)
	m := previous
	m.Target = commit(t, g, previous.Candidate, "human append", map[string]string{"human.txt": "human\n"})
	m.Main = commit(t, g, previous.Main, "new main", map[string]string{"main.txt": "new main\n"})
	m.SourceHead = commit(t, g, previous.SourceHead, "second source commit", map[string]string{"new.txt": "source addition\n"})
	must(t, g.replay(t.Context(), previous, &m, nil))
	for name, want := range map[string]string{"a.txt": "source", "fork.txt": "fork adaptation", "overlay.txt": "local overlay", "human.txt": "human", "main.txt": "new main", "new.txt": "source addition"} {
		if got := git(t, g, "show", m.Candidate+":"+name); got != want {
			t.Errorf("%s: got %q, want %q", name, got, want)
		}
	}
	if len(m.Mapping) != 2 || len(m.Overlays) != 2 {
		t.Fatalf("boundaries lost: %+v", m)
	}
	// Applying an empty upstream commit must retain its commit boundary.
	source := commit(t, g, m.SourceHead, "empty upstream commit", nil)
	next := m
	next.Target = m.Candidate
	next.SourceHead = source
	must(t, g.replay(t.Context(), m, &next, nil))
	if len(next.Mapping) != 3 {
		t.Fatal("empty source commit was dropped")
	}
}

func TestReplayRewriteReconcilesAdaptation(t *testing.T) {
	t.Run("fork behavior", func(t *testing.T) {
		g, previous := fixture(t)
		m := previous
		m.SourceHead = commit(t, g, previous.SourceBase, "squashed rewritten feature", map[string]string{"a.txt": "rewritten source\n"})
		calls := 0
		resolver := resolverFunc(func(_ context.Context, r resolveRequest, _ *ledger) (resolution, error) {
			calls++
			if !slices.Equal(r.Adaptations, []string{previous.SourceHead}) {
				t.Fatalf("missing adaptation request: %+v", r)
			}
			var file contextFile
			for _, f := range r.Files {
				if f.Path == "fork.txt" {
					file = f
				}
			}
			return resolution{Summary: "Preserve custom fork handling.", Adaptations: []adaptation{{Source: previous.SourceHead, Disposition: "retained", Reason: "The rewritten source does not contain the custom decoder integration."}}, Edits: []fileEdit{{Path: "fork.txt", Before: file.SHA256, Replacements: []replacement{{Old: "fork main", New: "fork adaptation"}}}}}, nil
		})
		must(t, g.replay(t.Context(), previous, &m, resolver))
		if calls != 1 || len(m.Adaptations) != 1 {
			t.Fatal("rewritten adaptation was silently discarded")
		}
		if got := git(t, g, "show", m.Candidate+":fork.txt"); got != "fork adaptation" {
			t.Fatal(got)
		}
		if got := git(t, g, "show", m.Candidate+":a.txt"); got != "rewritten source" {
			t.Fatal(got)
		}
		if len(m.Overlays) != 2 {
			t.Fatal("reconciliation must be a separate integration commit")
		}
	})
	for _, tc := range []struct {
		name, local, rewritten, want string
		stored, reuseFirst           bool
		calls                        int
	}{
		{name: "whitespace after another sync", local: "foobar", rewritten: "foo baz", want: "foobar", stored: true, reuseFirst: true, calls: 1},
		{name: "whitespace with stale flag", local: "foobar", rewritten: "foo baz", want: "foobar", calls: 1},
		{name: "identical deltas", local: "foo bar", rewritten: "foo baz", want: "foo baz", reuseFirst: true},
		{name: "known adaptation with identical deltas", local: "foo bar", rewritten: "foo baz", want: "foo bar", stored: true, reuseFirst: true, calls: 1},
		{name: "unique exact delta reuse", local: "foobar", rewritten: "foo bar", want: "foobar", reuseFirst: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, previous := fixture(t)
			previous.SourceHead = commit(t, g, previous.SourceBase, "upstream label", map[string]string{"value.go": "package value\nconst Label = \"foo bar\"\n"})
			previous.Candidate = commit(t, g, previous.Main, "fork label", map[string]string{"value.go": fmt.Sprintf("package value\nconst Label = %q\n", tc.local)})
			previous.Overlays = nil
			delta, err := g.delta(t.Context(), previous.SourceHead)
			must(t, err)
			previous.Mapping = []projection{{Source: previous.SourceHead, Local: previous.Candidate, Delta: delta, Adapted: tc.stored}}
			signing(t, &g)
			_, err = g.signCandidate(t.Context(), &previous)
			must(t, err)
			previous.Target = previous.Candidate
			receipt, err := g.seal(t.Context(), &previous, jobAPI(previous, nil))
			must(t, err)
			before, err := json.Marshal(previous)
			must(t, err)
			current := previous
			if tc.reuseFirst {
				next := current
				next.ID, next.Validation = "", nil
				must(t, g.replay(t.Context(), current, &next, resolverFunc(func(context.Context, resolveRequest, *ledger) (resolution, error) {
					t.Fatal("reusing unchanged source must not call the resolver")
					return resolution{}, errors.New("unexpected resolver call")
				})))
				if next.Mapping[0].Adapted != (tc.stored || tc.local != "foo bar") {
					t.Errorf("incorrect adaptation flag after reuse: %v", next.Mapping[0].Adapted)
				}
				current = next
			}
			m := current
			m.ID, m.Validation = "", nil
			m.Target = current.Candidate
			m.SourceHead = commit(t, g, current.SourceBase, "rewritten upstream label", map[string]string{"value.go": fmt.Sprintf("package value\nconst Label = %q\n", tc.rewritten)})
			calls := 0
			resolver := resolverFunc(func(_ context.Context, request resolveRequest, _ *ledger) (resolution, error) {
				calls++
				if !slices.Equal(request.Adaptations, []string{current.SourceHead}) {
					t.Fatalf("missing prior adaptation: %+v", request)
				}
				for _, f := range request.Files {
					if f.Path == "value.go" {
						return resolution{
							Summary:     "Preserve the fork label.",
							Adaptations: []adaptation{{Source: current.SourceHead, Disposition: "retained", Reason: "The fork label still applies."}},
							Edits:       []fileEdit{{Path: f.Path, Before: f.SHA256, Replacements: []replacement{{Old: fmt.Sprintf("%q", tc.rewritten), New: fmt.Sprintf("%q", tc.local)}}}},
						}, nil
					}
				}
				return resolution{}, errors.New("missing label context")
			})
			must(t, g.replay(t.Context(), current, &m, resolver))
			if calls != tc.calls || len(m.Adaptations) != tc.calls {
				t.Errorf("reconciliation calls: got %d, want %d; dispositions: %d", calls, tc.calls, len(m.Adaptations))
			}
			if got := git(t, g, "show", m.Candidate+":value.go"); got != fmt.Sprintf("package value\nconst Label = %q", tc.want) {
				t.Errorf("wrong label after replay: %s", got)
			}
			after, err := json.Marshal(previous)
			must(t, err)
			if string(before) != string(after) {
				t.Fatal("replay changed its signed input manifest")
			}
			must(t, g.verifyReceipt(t.Context(), receipt, previous))
		})
	}
}

func TestReplayExactDeltaRewriteRetainsProjection(t *testing.T) {
	g, previous := fixture(t)
	m := previous
	m.SourceHead = commit(t, g, previous.SourceBase, "new subject, identical exact delta", map[string]string{"a.txt": "source\n"})
	must(t, g.replay(t.Context(), previous, &m, nil))
	if got := git(t, g, "show", "-s", "--format=%s", m.Mapping[0].Local); got != "new subject, identical exact delta" {
		t.Fatalf("rewritten source message lost: %s", got)
	}
	if got := git(t, g, "show", m.Candidate+":fork.txt"); got != "fork adaptation" {
		t.Fatal(got)
	}
}

func TestReplayConflictUsesScopedEdits(t *testing.T) {
	g, previous := fixture(t)
	m := previous
	m.Main = commit(t, g, previous.Main, "conflicting main", map[string]string{"a.txt": "main side\n"})
	calls := 0
	resolver := resolverFunc(func(_ context.Context, r resolveRequest, _ *ledger) (resolution, error) {
		calls++
		var f contextFile
		for _, file := range r.Files {
			if file.Path == "a.txt" {
				f = file
			}
		}
		return resolution{Summary: "Integrate both changes.", Edits: []fileEdit{{Path: f.Path, Before: f.SHA256, Replacements: []replacement{{Old: f.Content, New: "main and source\n"}}}}}, nil
	})
	must(t, g.replay(t.Context(), previous, &m, resolver))
	if calls != 1 || git(t, g, "show", m.Candidate+":a.txt") != "main and source" {
		t.Fatal("conflict resolution was not applied")
	}
}

func TestRejectDivergedTargetAndMissingMapping(t *testing.T) {
	for _, mode := range []string{"target rewrite", "missing source", "wrong delta", "reordered overlays"} {
		t.Run(mode, func(t *testing.T) {
			g, previous := fixture(t)
			m := previous
			switch mode {
			case "target rewrite":
				m.Target = previous.Main
			case "missing source":
				previous.Mapping = nil
			case "wrong delta":
				previous.Mapping[0].Delta = strings.Repeat("a", 64)
			case "reordered overlays":
				previous.Overlays[0] = previous.Mapping[0].Local
			}
			if g.replay(t.Context(), previous, &m, nil) == nil {
				t.Fatal("invalid checkpoint accepted")
			}
		})
	}
}

func TestSignedPayloadAndNormalization(t *testing.T) {
	g, m := fixture(t)
	signing(t, &g)
	// Generated changes are accepted only in their fixed allowlist.
	write(t, g.dir, "go.sum", "example sum\n")
	git(t, g, "add", "go.sum")
	patch, err := g.command(t.Context(), nil, nil, "diff", "--cached", "--binary")
	must(t, err)
	git(t, g, "reset", "--hard", m.Candidate)
	patchFile := filepath.Join(t.TempDir(), "normalization.patch")
	must(t, os.WriteFile(patchFile, patch, 0o600))
	must(t, g.applyNormalization(t.Context(), &m, patchFile))
	if got := git(t, g, "show", m.Candidate+":go.sum"); got != "example sum" {
		t.Fatal(got)
	}
	envelope, err := g.signCandidate(t.Context(), &m)
	must(t, err)
	must(t, g.verifyCandidate(t.Context(), m))
	dir := t.TempDir()
	must(t, g.export(t.Context(), dir, m, envelope))
	imported, got, err := importPayload(t.Context(), filepath.Join(t.TempDir(), "repo"), dir, g.dir, g.name, g.email, g.allowedSigners, envelope)
	must(t, err)
	if !reflect.DeepEqual(got, m) {
		t.Fatal("manifest changed")
	}
	must(t, imported.verifyCandidate(t.Context(), got))
	got.Candidate = got.Main
	must(t, writeJSON(filepath.Join(dir, "manifest.json"), got))
	if _, _, err = importPayload(t.Context(), filepath.Join(t.TempDir(), "repo"), dir, g.dir, g.name, g.email, g.allowedSigners, envelope); err == nil {
		t.Fatal("manifest substitution accepted")
	}
}

func TestRejectProtectedHistoryAndNormalization(t *testing.T) {
	g, m := fixture(t)
	bad := commit(t, g, m.Candidate, "upstream automation", map[string]string{".github/workflows/evil.yml": "run: exfiltrate\n"})
	if g.checkPolicy(t.Context(), m.Main, bad) == nil {
		t.Fatal("untrusted workflow accepted")
	}
	patch, err := g.command(t.Context(), nil, nil, "diff", "--binary", m.Candidate, bad)
	must(t, err)
	git(t, g, "checkout", "--quiet", "--detach", m.Candidate)
	file := filepath.Join(t.TempDir(), "patch")
	must(t, os.WriteFile(file, patch, 0o600))
	if g.applyNormalization(t.Context(), &m, file) == nil {
		t.Fatal("normalizer changed trusted workflow")
	}
}

func TestRejectRenamedProtectedHistory(t *testing.T) {
	g, m := fixture(t)
	git(t, g, "mv", ".github/workflows/ci.yml", "renamed.yml")
	must(t, g.integrate(t.Context(), &m, "rename protected workflow"))
	if g.checkPolicy(t.Context(), m.Main, m.Candidate) == nil {
		t.Error("renaming a protected workflow outside .github bypassed policy")
	}
	signing(t, &g)
	if _, err := g.signCandidate(t.Context(), &m); err == nil {
		t.Error("signed a candidate missing a protected workflow")
	}
}

func TestRejectNormalizationRename(t *testing.T) {
	for _, source := range []string{"a.txt", ".github/workflows/ci.yml"} {
		t.Run(source, func(t *testing.T) {
			g, m := fixture(t)
			git(t, g, "mv", source, "go.sum")
			patch, err := g.command(t.Context(), nil, nil, "diff", "--cached", "--binary", "--find-renames")
			must(t, err)
			git(t, g, "reset", "--hard", m.Candidate)
			file := filepath.Join(t.TempDir(), "normalization.patch")
			must(t, os.WriteFile(file, patch, 0o600))
			if g.applyNormalization(t.Context(), &m, file) == nil {
				t.Fatal("normalization deleted a non-generated file by renaming it to an allowed path")
			}
		})
	}
}

func TestNewGeneratedFiles(t *testing.T) {
	for _, mode := range []string{"missing", "normalized"} {
		t.Run(mode, func(t *testing.T) {
			g, m := fixture(t)
			next := commit(t, g, m.Candidate, "ignore build output", map[string]string{".gitignore": "/build-output\n/go.work.sum\n"})
			m.Overlays = append(m.Overlays, next)
			m.Candidate = next
			bin := t.TempDir()
			for _, name := range []string{"go", "make"} {
				write(t, bin, name, `#!/bin/sh
mkdir -p docs/command-line
printf 'generated\n' > docs/command-line/prometheus.md
printf 'build output\n' > build-output
printf 'workspace sums\n' > go.work.sum
`)
				must(t, os.Chmod(filepath.Join(bin, name), 0o755))
			}
			t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
			if mode == "normalized" {
				out := t.TempDir()
				must(t, g.normalize(t.Context(), out))
				git(t, g, "reset", "--hard", m.Candidate)
				git(t, g, "clean", "-fd")
				must(t, g.applyNormalization(t.Context(), &m, filepath.Join(out, "normalization.patch")))
				if got := git(t, g, "show", m.Candidate+":docs/command-line/prometheus.md"); got != "generated" {
					t.Fatalf("normalization omitted the generated document: %q", got)
				}
				for _, ignored := range []string{"build-output", "go.work.sum"} {
					if git(t, g, "ls-tree", "--name-only", m.Candidate, "--", ignored) != "" {
						t.Fatalf("normalization included ignored output %s", ignored)
					}
				}
			}
			signing(t, &g)
			_, err := g.signCandidate(t.Context(), &m)
			must(t, err)
			err = g.validate(t.Context(), m, "generated", filepath.Join(t.TempDir(), "generated.log"))
			if mode == "normalized" {
				must(t, err)
			} else if err == nil {
				t.Fatal("generated validation accepted a candidate missing the generated document")
			}
		})
	}
}

func TestEditsAreAtomicAndBounded(t *testing.T) {
	for _, mode := range []string{"stale hash", "traversal", "duplicate edit", "ambiguous text", "overlap", "test removal", "skip test", "symlink", "protected path"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			before := "package example\nfunc TestBehavior(t *testing.T) { check() }\n"
			name := "example_test.go"
			write(t, dir, name, before)
			files, err := collectFiles(dir, []string{name})
			must(t, err)
			edit := fileEdit{Path: name, Before: digest([]byte(before)), Replacements: []replacement{{Old: "check()", New: "betterCheck()"}}}
			result := resolution{Edits: []fileEdit{edit}}
			switch mode {
			case "stale hash":
				result.Edits[0].Before = strings.Repeat("0", 64)
			case "traversal":
				result.Edits[0].Path = "../escape.go"
			case "duplicate edit":
				result.Edits = append(result.Edits, edit)
			case "ambiguous text":
				result.Edits[0].Replacements[0].Old = "e"
			case "overlap":
				result.Edits[0].Replacements = append(result.Edits[0].Replacements, replacement{Old: "check", New: "other"})
			case "test removal":
				result.Edits[0].Replacements = []replacement{{Old: before, New: "package example\n"}}
			case "skip test":
				result.Edits[0].Replacements[0].New = "t.Skip()"
			case "symlink":
				must(t, os.Remove(filepath.Join(dir, name)))
				write(t, dir, "target", before)
				must(t, os.Symlink(filepath.Join(dir, "target"), filepath.Join(dir, name)))
			case "protected path":
				result.Edits[0].Path = ".github/workflows/ci.yml"
			}
			if _, err = applyResolution(dir, files, result); err == nil {
				t.Fatal("unsafe edit accepted")
			}
			b, err := os.ReadFile(filepath.Join(dir, name))
			must(t, err)
			if string(b) != before {
				t.Fatal("failed edit modified file")
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(status int, body any) *http.Response {
	data, _ := json.Marshal(body)
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(data)))}
}

func TestClaudeLedgerSurvivesFailuresAndRepairs(t *testing.T) {
	for _, tc := range []struct {
		name          string
		input, output int
		calls         int
		total         int64
		ok            bool
	}{
		{name: "success", input: 100, output: 100, calls: 1, total: 2400, ok: true},
		{name: "maximum usage", input: 1_000_000, output: 32768, calls: 1, total: 4_655_360, ok: true},
		{name: "legacy ledger", input: 100, output: 100, calls: 1, total: 6_105_400, ok: true},
		{name: "truncated", input: 100, output: 100, calls: 1, total: 2400},
		{name: "refusal", input: 100, output: 100, calls: 1, total: 2400},
		{name: "invalid JSON", input: 100, output: 100, calls: 1, total: 2400},
		{name: "network loss", calls: 1, total: 6_100_000},
		{name: "retry", input: 100, output: 100, calls: 2, total: 6_102_400, ok: true},
		{name: "unknown usage", output: 100, calls: 1, total: 6_100_000},
		{name: "budget", total: 48_800_000},
		{name: "rerun"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			l := ledger{RunID: 42, Attempt: 1}
			switch tc.name {
			case "rerun":
				l.Attempt = 2
			case "budget":
				for range 8 {
					l.Charges = append(l.Charges, charge{Reserved: reservationMicroUSD})
				}
			case "legacy ledger":
				// Preserve an Opus 5 charge and an attempt with unknown usage.
				actual := int64(3000)
				l.Charges = []charge{
					{Reserved: 6_100_000, Actual: &actual, RequestID: "legacy-known"},
					{Reserved: 6_100_000, RequestID: "legacy-unknown"},
				}
			}
			initialCharges := len(l.Charges)
			history, err := json.Marshal(l.Charges)
			must(t, err)
			ledgerFile := filepath.Join(t.TempDir(), "ledger.json")
			must(t, writeJSON(ledgerFile, l))
			stored, err := os.ReadFile(ledgerFile)
			must(t, err)
			must(t, decodeJSON(stored, &l))
			calls := 0
			c := claude{key: "test-key", endpoint: "https://claude.invalid/messages", ledgerPath: ledgerFile}
			c.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				stored, err := os.ReadFile(ledgerFile)
				must(t, err)
				var disk ledger
				must(t, decodeJSON(stored, &disk))
				if len(disk.Charges) != initialCharges+calls || disk.Charges[initialCharges+calls-1].Actual != nil {
					t.Fatal("request sent before durable reservation")
				}
				if req.Header.Get("x-api-key") != "test-key" {
					t.Fatal("missing API credential")
				}
				var body map[string]json.RawMessage
				must(t, json.NewDecoder(req.Body).Decode(&body))
				for field, want := range map[string]string{
					"model": `"claude-opus-5-5"`, "max_tokens": "32768",
					"service_tier": `"standard_only"`, "stream": "true",
				} {
					if string(body[field]) != want {
						t.Errorf("request %s = %s, want %s", field, body[field], want)
					}
				}
				var output struct {
					Effort string
					Format struct {
						Type   string
						Schema json.RawMessage
					}
				}
				must(t, json.Unmarshal(body["output_config"], &output))
				if output.Effort != "high" || output.Format.Type != "json_schema" || len(output.Format.Schema) == 0 {
					t.Errorf("unexpected output configuration: %s", body["output_config"])
				}
				for _, field := range []string{"thinking", "tools", "tool_choice"} {
					if _, ok := body[field]; ok {
						t.Errorf("unexpected request field %s", field)
					}
				}
				if tc.name == "network loss" {
					return nil, errors.New("connection lost")
				}
				if tc.name == "retry" && calls == 1 {
					return response(503, map[string]any{}), nil
				}
				stop := "end_turn"
				result, err := json.Marshal(resolution{Summary: "Resolved.", Edits: []fileEdit{}, Adaptations: []adaptation{}})
				must(t, err)
				text := string(result)
				switch tc.name {
				case "truncated":
					stop = "max_tokens"
				case "refusal":
					stop = "refusal"
					text = "I cannot resolve this request."
				case "invalid JSON":
					text = "invalid JSON"
				}
				return response(200, map[string]any{"stop_reason": stop, "content": []map[string]string{{"type": "thinking"}, {"type": "text", "text": text}}, "usage": map[string]int{"input_tokens": tc.input, "output_tokens": tc.output}}), nil
			})}
			resolved, err := c.resolve(t.Context(), resolveRequest{Task: "test"}, &l)
			if (err == nil) != tc.ok {
				t.Fatalf("unexpected result: %v", err)
			}
			if tc.name == "refusal" && !reflect.DeepEqual(resolved, resolution{}) {
				t.Fatal("refusal returned a resolution")
			}
			if calls != tc.calls {
				t.Fatalf("sent %d requests, want %d", calls, tc.calls)
			}
			if l.total() != tc.total {
				t.Errorf("ledger total = %d, want %d", l.total(), tc.total)
			}
			stored, err = os.ReadFile(ledgerFile)
			must(t, err)
			var disk ledger
			must(t, decodeJSON(stored, &disk))
			must(t, disk.valid())
			if !reflect.DeepEqual(disk, l) {
				t.Fatal("ledger changes were not persisted")
			}
			if initialCharges > 0 {
				preserved, err := json.Marshal(disk.Charges[:initialCharges])
				must(t, err)
				if string(preserved) != string(history) {
					t.Fatal("historical charges changed")
				}
			}
			if tc.calls == 0 {
				return
			}
			// A subsequent repair inherits every charge, including unknown attempts.
			if _, err = l.reserve(); err != nil {
				t.Fatal(err)
			}
			if len(l.Charges) != initialCharges+calls+1 {
				t.Fatal("ledger reset")
			}
		})
	}
}

func TestAtomicPublicationLeases(t *testing.T) {
	g, m := fixture(t)
	remote := filepath.Join(t.TempDir(), "remote.git")
	git(t, g, "init", "--bare", "--quiet", remote)
	git(t, g, "push", remote, m.Target+":"+m.TargetRef)
	next := commit(t, g, m.Target, "candidate", map[string]string{"next.txt": "next\n"})
	human := commit(t, g, m.Target, "concurrent human", map[string]string{"human.txt": "human\n"})
	git(t, g, "push", remote, human+":"+m.TargetRef)
	recovery := botPrefix + "recovery/" + strings.Repeat("a", 64)
	err := g.atomicPush(t.Context(), remote, []refUpdate{{m.TargetRef, m.Target, next}, {stateRef, "", next}, {recovery, "", m.Target}})
	if err == nil {
		t.Fatal("stale lease accepted")
	}
	got, err := g.remoteRef(t.Context(), remote, m.TargetRef)
	must(t, err)
	if got != human {
		t.Fatal("concurrent human commit overwritten")
	}
	for _, ref := range []string{stateRef, recovery} {
		got, err = g.remoteRef(t.Context(), remote, ref)
		must(t, err)
		if got != "" {
			t.Fatal("atomic push partially updated")
		}
	}
	must(t, g.atomicPush(t.Context(), remote, []refUpdate{{m.TargetRef, human, next}, {stateRef, "", next}, {recovery, "", human}}))
}

func jobAPI(m manifest, mutate func(map[string]any)) github {
	return github{endpoint: "https://github.invalid", token: "test", client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if strings.Contains(req.URL.Path, "/jobs") {
			var jobs []map[string]any
			for i, c := range checks {
				job := map[string]any{"id": i + 1, "run_id": m.Ledger.RunID, "name": fmt.Sprintf("validate-%d / native-metadata/%d/%s", m.Round, m.Round, c), "status": "completed", "conclusion": "success", "head_sha": m.Control, "run_attempt": 1}
				if mutate != nil {
					mutate(job)
				}
				jobs = append(jobs, job)
			}
			return response(200, map[string]any{"jobs": jobs}), nil
		}
		return response(200, map[string]any{"id": m.Ledger.RunID, "head_sha": m.Control, "head_branch": "main", "path": ".github/workflows/sync-native-metadata.yml", "run_attempt": 1, "event": "workflow_dispatch", "repository": map[string]string{"full_name": repository}}), nil
	})}}
}

func validationAPI(t *testing.T, m manifest, mode string) github {
	t.Helper()
	a := jobAPI(m, func(job map[string]any) {
		if mode == "validation failed" {
			job["conclusion"] = "failure"
		}
	})
	transport := a.client.Transport
	a.token = "validation-token"
	a.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if req.Header.Get("Authorization") != "Bearer validation-token" || req.Method != http.MethodGet {
			t.Fatal("validation must use the built-in token for reads only")
		}
		stage := "run"
		prefix := fmt.Sprintf("/repos/%s/actions/runs/%d", repository, m.Ledger.RunID)
		switch req.URL.Path {
		case prefix + "/attempts/1":
		case prefix + "/attempts/1/jobs":
			stage = "jobs"
			if strings.HasPrefix(mode, "jobs ") && req.URL.Query().Get("page") == "1" {
				resp, err := transport.RoundTrip(req)
				must(t, err)
				defer resp.Body.Close()
				var page struct {
					Jobs []map[string]any `json:"jobs"`
				}
				must(t, json.NewDecoder(resp.Body).Decode(&page))
				// Fail after a full page containing every required successful job.
				for len(page.Jobs) < 100 {
					page.Jobs = append(page.Jobs, map[string]any{"name": "unrelated job"})
				}
				return response(200, page), nil
			}
		default:
			t.Fatalf("unexpected validation route: %s", req.URL.Path)
		}
		switch mode {
		case stage + " unauthorized":
			return response(401, nil), nil
		case stage + " forbidden":
			return response(403, nil), nil
		}
		return transport.RoundTrip(req)
	})}
	return a
}

func TestValidationUsesExactJobsAndAttempt(t *testing.T) {
	_, m := fixture(t)
	for _, mode := range []string{"success", "failed job", "wrong SHA", "wrong round", "wrong attempt", "wrong job run", "duplicate", "missing job", "wrong run", "wrong repository", "wrong workflow", "wrong branch", "wrong controller", "wrong event", "wrong run attempt"} {
		t.Run(mode, func(t *testing.T) {
			a := jobAPI(m, func(job map[string]any) {
				switch mode {
				case "failed job":
					job["conclusion"] = "failure"
				case "wrong SHA":
					job["head_sha"] = strings.Repeat("a", 40)
				case "wrong round":
					job["name"] = "validate-1 / native-metadata/1/go"
				case "wrong attempt":
					job["run_attempt"] = 2
				case "wrong job run":
					job["run_id"] = m.Ledger.RunID + 1
				case "duplicate":
					job["name"] = "validate-0 / native-metadata/0/go"
				}
			})
			transport := a.client.Transport
			a.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				prefix := fmt.Sprintf("/repos/%s/actions/runs/%d/attempts/1", repository, m.Ledger.RunID)
				if req.URL.Path != prefix && req.URL.Path != prefix+"/jobs" {
					t.Fatalf("validation did not pin attempt 1: %s", req.URL.Path)
				}
				resp, e := transport.RoundTrip(req)
				if e != nil {
					return nil, e
				}
				defer resp.Body.Close()
				var body map[string]any
				must(t, json.NewDecoder(resp.Body).Decode(&body))
				if req.URL.Path == prefix {
					switch mode {
					case "wrong run":
						body["id"] = m.Ledger.RunID + 1
					case "wrong repository":
						body["repository"] = map[string]string{"full_name": "another/repo"}
					case "wrong workflow":
						body["path"] = ".github/workflows/other.yml"
					case "wrong branch":
						body["head_branch"] = topic
					case "wrong controller":
						body["head_sha"] = m.Target
					case "wrong event":
						body["event"] = "pull_request"
					case "wrong run attempt":
						body["run_attempt"] = 2
					}
				} else if mode == "missing job" {
					body["jobs"] = body["jobs"].([]any)[1:]
				}
				return response(200, body), nil
			})}
			_, err := a.jobs(t.Context(), m)
			if (err == nil) != (mode == "success") {
				t.Fatalf("unexpected result: %v", err)
			}
		})
	}
}

func TestSealBindsHistoryAndValidation(t *testing.T) {
	g, m := fixture(t)
	signing(t, &g)
	_, err := g.signCandidate(t.Context(), &m)
	must(t, err)
	receipt, err := g.seal(t.Context(), &m, jobAPI(m, nil))
	must(t, err)
	must(t, g.verifyReceipt(t.Context(), receipt, m))
	if git(t, g, "ls-tree", "-r", "--name-only", receipt) != "manifest.json" {
		t.Fatal("checkpoint exposes upstream workflows")
	}
	for _, tip := range []string{m.Target, m.Candidate, m.SourceHead} {
		ok, e := g.ancestor(t.Context(), tip, receipt)
		must(t, e)
		if !ok {
			t.Fatal("checkpoint lost captured history")
		}
	}
	for _, mode := range []string{"candidate", "version", "jobs", "parents", "signature"} {
		t.Run(mode, func(t *testing.T) {
			changed, envelope := m, receipt
			switch mode {
			case "candidate":
				changed.Candidate = m.Main
			case "version":
				changed.Version = 1
			case "jobs":
				v := *m.Validation
				v.Round++
				changed.Validation = &v
			case "parents":
				envelope, err = g.metadata(t.Context(), m, []string{m.Candidate})
				must(t, err)
			case "signature":
				envelope, err = g.commitTree(t.Context(), git(t, g, "rev-parse", receipt+"^{tree}"), receiptParents(m), "unsigned", nil, testDate, false)
				must(t, err)
			}
			if g.verifyReceipt(t.Context(), envelope, changed) == nil {
				t.Fatal("invalid receipt accepted")
			}
		})
	}
}

func TestPublishValidatedCandidateAndRetry(t *testing.T) {
	for _, mode := range []string{
		"success", "round 1", "round 2", "existing candidate", "stale target", "stale state", "changed policy",
		"validation failed", "run unauthorized", "run forbidden", "jobs unauthorized", "jobs forbidden",
		"wrong receipt", "missing recovery", "orphan recovery", "changed candidate", "cancel before push",
		"race during validation", "lost acknowledgement", "readback unavailable", "cancel after push", "report failure",
	} {
		t.Run(mode, func(t *testing.T) {
			g, m := fixture(t)
			signing(t, &g)
			if mode == "round 1" {
				m.Round = 1
			}
			if mode == "round 2" {
				m.Round = 2
			}
			_, err := g.signCandidate(t.Context(), &m)
			must(t, err)
			receipt, err := g.seal(t.Context(), &m, jobAPI(m, nil))
			must(t, err)
			remote := filepath.Join(t.TempDir(), "remote.git")
			git(t, g, "init", "--quiet", "--bare", remote)
			git(t, g, "push", remote, m.Main+":refs/heads/main", m.Target+":"+m.TargetRef)
			receiptRef := botPrefix + "receipt/" + m.ID
			candidateRef := botPrefix + "candidate/" + m.Candidate
			recoveryRef := botPrefix + "recovery/" + m.ID
			human := commit(t, g, m.Target, "human advance", map[string]string{"human.txt": "human\n"})
			switch mode {
			case "existing candidate":
				git(t, g, "push", remote, m.Candidate+":"+candidateRef)
			case "stale target":
				git(t, g, "push", remote, human+":"+m.TargetRef)
			case "stale state":
				git(t, g, "push", remote, m.Main+":"+stateRef)
			case "changed policy":
				changed := commit(t, g, m.Main, "new policy", map[string]string{".github/workflows/ci.yml": "changed\n"})
				git(t, g, "push", remote, changed+":refs/heads/main")
			case "wrong receipt":
				git(t, g, "push", remote, m.Main+":"+receiptRef)
			case "missing recovery":
				git(t, g, "push", remote, receipt+":"+receiptRef, m.Candidate+":"+candidateRef)
			case "orphan recovery":
				git(t, g, "push", remote, m.Target+":"+recoveryRef)
			case "changed candidate":
				git(t, g, "push", remote, m.Main+":"+candidateRef)
			}
			before := git(t, g, "ls-remote", "--refs", remote)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			jobs := validationAPI(t, m, mode)
			if mode == "race during validation" || mode == "cancel before push" {
				transport, once := jobs.client.Transport, false
				jobs.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
					if !once {
						once = true
						if mode == "race during validation" {
							git(t, g, "push", remote, human+":"+m.TargetRef)
							before = git(t, g, "ls-remote", "--refs", remote)
						} else {
							cancel()
						}
					}
					return transport.RoundTrip(req)
				})}
			}
			if slices.Contains([]string{"lost acknowledgement", "readback unavailable", "cancel after push"}, mode) {
				realGit, e := exec.LookPath("git")
				must(t, e)
				dir := t.TempDir()
				write(t, dir, "git", `#!/bin/bash
for arg in "$@"; do
  if [[ "$arg" == push ]]; then
    "$SYNC_TEST_GIT" "$@" || exit $?
    if [[ "$SYNC_TEST_MODE" == 'readback unavailable' ]]; then mv "$SYNC_TEST_REMOTE" "$SYNC_TEST_REMOTE.hidden"; fi
    if [[ "$SYNC_TEST_MODE" == 'cancel after push' ]]; then
      touch "$SYNC_TEST_MARKER"
      exec sleep 20
    fi
    exit 1
  fi
done
exec "$SYNC_TEST_GIT" "$@"
`)
				must(t, os.Chmod(filepath.Join(dir, "git"), 0o700))
				t.Setenv("SYNC_TEST_GIT", realGit)
				t.Setenv("SYNC_TEST_MODE", mode)
				t.Setenv("SYNC_TEST_REMOTE", remote)
				marker := filepath.Join(dir, "accepted")
				t.Setenv("SYNC_TEST_MARKER", marker)
				t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
				if mode == "cancel after push" {
					done := make(chan struct{})
					defer close(done)
					go func() {
						ticker := time.NewTicker(10 * time.Millisecond)
						defer ticker.Stop()
						for {
							select {
							case <-done:
								return
							case <-ticker.C:
								if _, e := os.Stat(marker); e == nil {
									cancel()
									return
								}
							}
						}
					}()
				}
			}
			status, err := g.publish(ctx, remote, receipt, m, jobs)
			published := slices.Contains([]string{"success", "round 1", "round 2", "existing candidate", "lost acknowledgement", "readback unavailable", "cancel after push", "report failure"}, mode)
			if (err == nil) != (published && mode != "readback unavailable") {
				t.Fatalf("unexpected publication outcome %s: %v", status, err)
			}
			if mode == "readback unavailable" {
				if status != "unknown" {
					t.Fatal("uncertain push was reported as failure to publish")
				}
				must(t, os.Rename(remote+".hidden", remote))
			}
			if !published {
				if git(t, g, "ls-remote", "--refs", remote) != before {
					t.Fatal("rejected publication modified remote state")
				}
				return
			}
			for ref, want := range map[string]string{m.TargetRef: m.Candidate, stateRef: receipt, receiptRef: receipt, candidateRef: m.Candidate, recoveryRef: m.Target} {
				got, e := g.remoteRef(t.Context(), remote, ref)
				must(t, e)
				if got != want {
					t.Fatalf("%s: got %s, want %s", ref, got, want)
				}
			}
			if mode == "report failure" {
				blocked := filepath.Join(t.TempDir(), "not-a-directory")
				must(t, os.WriteFile(blocked, []byte("blocked"), 0o600))
				if g.report(t.Context(), blocked, m, status) == nil {
					t.Fatal("report failure ignored")
				}
			}
			jobs.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				t.Fatal("completed publication queried Actions")
				return nil, errors.New("unexpected Actions read")
			})}
			for range 2 {
				status, err = g.publish(t.Context(), remote, receipt, m, jobs)
				must(t, err)
				if status != "already-published" {
					t.Fatalf("retry: %s", status)
				}
			}
		})
	}
	t.Run("later human work, sync, and rollback", func(t *testing.T) {
		g, m := fixture(t)
		signing(t, &g)
		_, err := g.signCandidate(t.Context(), &m)
		must(t, err)
		receipt, err := g.seal(t.Context(), &m, jobAPI(m, nil))
		must(t, err)
		remote := filepath.Join(t.TempDir(), "remote.git")
		git(t, g, "init", "--quiet", "--bare", remote)
		git(t, g, "push", remote, m.Main+":refs/heads/main", m.Target+":"+m.TargetRef)
		_, err = g.publish(t.Context(), remote, receipt, m, validationAPI(t, m, "success"))
		must(t, err)
		later := commit(t, g, m.Candidate, "later human", map[string]string{"later.txt": "preserve\n"})
		git(t, g, "push", remote, later+":"+m.TargetRef)
		noAPI := github{endpoint: "https://github.invalid", client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Fatal("completed publication queried Actions")
			return nil, errors.New("unexpected Actions read")
		})}}
		for _, stage := range []string{"human", "sync", "rollback"} {
			switch stage {
			case "sync":
				next := m
				next.Mapping = slices.Clone(m.Mapping)
				next.Target = later
				next.Candidate = later
				next.Overlays = append(slices.Clone(m.Overlays), later)
				next.PreviousState = receipt
				next.PreviousSourceHead = m.SourceHead
				next.Created = "2026-09-19T00:00:00Z"
				next.Ledger.RunID++
				next.Validation = nil
				next.ID = ""
				_, err = g.signCandidate(t.Context(), &next)
				must(t, err)
				nextReceipt, e := g.seal(t.Context(), &next, jobAPI(next, nil))
				must(t, e)
				_, err = g.publish(t.Context(), remote, nextReceipt, next, validationAPI(t, next, "success"))
				must(t, err)
			case "rollback":
				git(t, g, "push", "--atomic", "--force", remote, m.Target+":"+m.TargetRef, ":"+stateRef)
			}
			before := git(t, g, "ls-remote", "--refs", remote)
			status, e := g.publish(t.Context(), remote, receipt, m, noAPI)
			must(t, e)
			if status != "already-published" || git(t, g, "ls-remote", "--refs", remote) != before {
				t.Fatalf("retry changed %s history", stage)
			}
		}
	})
}

func TestWorkflowCredentialBoundaries(t *testing.T) {
	t.Run("attempt guards stop before setup", func(t *testing.T) {
		t.Setenv("GITHUB_REPOSITORY", repository)
		t.Setenv("GITHUB_REF", "refs/heads/main")
		t.Setenv("GITHUB_SHA", strings.Repeat("a", 40))
		t.Setenv("GITHUB_RUN_ATTEMPT", "2")
		for _, command := range []string{"prepare", "normalize", "sign", "validate", "repair", "seal"} {
			t.Run(command, func(t *testing.T) {
				dir := t.TempDir()
				err := run(t.Context(), []string{command, "--work", filepath.Join(dir, "work"), "--out", filepath.Join(dir, "out")})
				if err == nil || !strings.Contains(err.Error(), "reruns are disabled") {
					t.Fatalf("rerun accepted: %v", err)
				}
				entries, e := os.ReadDir(dir)
				must(t, e)
				if len(entries) != 0 {
					t.Fatal("rerun performed setup")
				}
			})
		}
	})
	t.Run("publication input and outcomes", func(t *testing.T) {
		data, err := os.ReadFile("../../.github/workflows/sync-native-metadata.yml")
		must(t, err)
		text := string(data)
		for _, name := range []string{"Require exact sealed input", "Report sync outcome"} {
			_, tail, ok := strings.Cut(text, "      - name: "+name+"\n")
			if !ok {
				t.Fatalf("missing %s", name)
			}
			_, tail, ok = strings.Cut(tail, "        run: |\n")
			if !ok {
				t.Fatal("missing run block")
			}
			var script strings.Builder
			for line := range strings.SplitSeq(tail, "\n") {
				if !strings.HasPrefix(line, "          ") {
					break
				}
				script.WriteString(strings.TrimPrefix(line, "          ") + "\n")
			}
			if name == "Require exact sealed input" {
				for _, tc := range []struct {
					id, envelope string
					valid        bool
				}{
					{"123", strings.Repeat("a", 40), true},
					{"", strings.Repeat("a", 40), false},
					{"0", strings.Repeat("a", 40), false},
					{"1,2", strings.Repeat("a", 40), false},
					{"123x", strings.Repeat("a", 40), false},
					{"123", "", false},
					{"123", "main", false},
				} {
					cmd := exec.CommandContext(t.Context(), "bash", "-e", "-c", script.String())
					cmd.Env = []string{"ARTIFACT_ID=" + tc.id, "ENVELOPE=" + tc.envelope}
					if err = cmd.Run(); (err == nil) != tc.valid {
						t.Fatalf("input %q/%q: %v", tc.id, tc.envelope, err)
					}
				}
				continue
			}
			for _, tc := range []struct {
				name, prepare, noop, seal, publish, outcome, dry, disabled string
				success                                                    bool
			}{
				{name: "disabled", prepare: "skipped", disabled: "true", success: true},
				{name: "unchanged", prepare: "success", noop: "true", success: true},
				{name: "dry run", prepare: "success", seal: "success", dry: "true", success: true},
				{name: "published", prepare: "success", seal: "success", publish: "success", outcome: "published", success: true},
				{name: "retry", prepare: "success", seal: "success", publish: "success", outcome: "already-published", success: true},
				{name: "skipped publisher", prepare: "success", seal: "success", publish: "skipped"},
				{name: "unknown", prepare: "success", seal: "success", publish: "failure", outcome: "unknown"},
				{name: "report failure", prepare: "success", seal: "success", publish: "failure", outcome: "published"},
				{name: "failed dry run", prepare: "failure", seal: "skipped", dry: "true"},
				{name: "whole rerun", prepare: "skipped", seal: "skipped", publish: "skipped"},
			} {
				t.Run(tc.name, func(t *testing.T) {
					summary := filepath.Join(t.TempDir(), "summary")
					cmd := exec.CommandContext(t.Context(), "bash", "-e", "-c", script.String())
					cmd.Env = []string{"GITHUB_STEP_SUMMARY=" + summary, "PREPARE=" + tc.prepare, "NOOP=" + tc.noop, "SEAL=" + tc.seal, "PUBLISH=" + tc.publish, "PUBLICATION=" + tc.outcome, "DRY_RUN=" + tc.dry, "DISABLED=" + tc.disabled, "CANCELLED=false"}
					if e := cmd.Run(); (e == nil) != tc.success {
						t.Fatalf("outcome: %v", e)
					}
					body, e := os.ReadFile(summary)
					must(t, e)
					if len(body) == 0 || (tc.outcome != "" && !strings.Contains(string(body), tc.outcome)) {
						t.Fatalf("missing publication outcome: %s", body)
					}
				})
			}
		}
	})
	t.Run("publication credentials required before setup", func(t *testing.T) {
		t.Setenv("GITHUB_REPOSITORY", repository)
		t.Setenv("GITHUB_REF", "refs/heads/main")
		t.Setenv("GITHUB_SHA", strings.Repeat("a", 40))
		t.Setenv("GITHUB_RUN_ATTEMPT", "1")
		for _, command := range []string{"publish"} {
			for _, missing := range []string{"GH_TOKEN", "GITHUB_TOKEN"} {
				for _, mode := range []string{"unset", "empty"} {
					t.Run(command+"/"+missing+"/"+mode, func(t *testing.T) {
						t.Setenv("GH_TOKEN", "app-token")
						t.Setenv("GITHUB_TOKEN", "validation-token")
						t.Setenv(missing, "")
						if mode == "unset" {
							must(t, os.Unsetenv(missing))
						}
						dir := t.TempDir()
						t.Setenv("TMPDIR", dir)
						err := run(t.Context(), []string{command, "--work", filepath.Join(dir, "work"), "--out", filepath.Join(dir, "out")})
						if err == nil || err.Error() != missing+" is required for "+command {
							t.Fatalf("missing credential did not fail before setup: %v", err)
						}
						entries, err := os.ReadDir(dir)
						must(t, err)
						if len(entries) != 0 {
							t.Fatal("missing credential created scratch files")
						}
					})
				}
			}
		}
	})
	t.Run("subprocess credentials", func(t *testing.T) {
		t.Setenv("GH_TOKEN", "app-token")
		t.Setenv("GITHUB_TOKEN", "validation-token")
		for _, variable := range commandEnv() {
			name, _, _ := strings.Cut(variable, "=")
			if name == "GH_TOKEN" || name == "GITHUB_TOKEN" {
				t.Fatal("subprocess inherited a GitHub token variable")
			}
		}
		g, err := initRepo(t.Context(), t.TempDir(), "Sync Bot", "bot@example.invalid")
		must(t, err)
		g.token = os.Getenv("GH_TOKEN")
		header := git(t, g, "config", "--get", "http.https://github.com/.extraheader")
		if header != "AUTHORIZATION: basic "+base64.StdEncoding.EncodeToString([]byte("x-access-token:app-token")) {
			t.Fatal("Git did not receive the App token explicitly")
		}
	})
	root := "../.."
	jobPattern := regexp.MustCompile(`(?m)^  ([a-z][a-z0-9-]*):\n`)
	permissionPattern := regexp.MustCompile(`(?m)^    permissions:\n(?:      [^\n]+\n)+`)
	for _, name := range []string{"sync-native-metadata.yml", "validate-native-metadata-sync.yml", "native-metadata-sync-ci.yml"} {
		b, err := os.ReadFile(filepath.Join(root, ".github/workflows", name))
		must(t, err)
		text := string(b)
		lines := strings.Split(text, "\n")
		for i, line := range lines {
			if strings.Contains(line, "artifact-ids:") && (i+1 == len(lines) || strings.TrimSpace(lines[i+1]) != "merge-multiple: true") {
				t.Fatalf("%s: downloading by ID must extract to the requested directory", name)
			}
		}
		if !strings.Contains(text, "\ncache-mode: none\n") {
			t.Fatalf("%s permits shared caches", name)
		}
		if strings.Contains(text, "secrets: inherit") || strings.Contains(text, "persist-credentials: true") {
			t.Fatalf("%s leaks credentials", name)
		}
		if name == "validate-native-metadata-sync.yml" {
			if strings.Contains(text, "secrets.") || strings.Contains(text, "id-token:") || strings.Contains(text, ": write") {
				t.Fatal("validator has privileged credentials")
			}
			if !strings.Contains(text, "fail-fast: false") || !strings.Contains(text, "matrix.check") {
				t.Fatal("validator is not a complete matrix")
			}
		}
		if name == "sync-native-metadata.yml" {
			if !strings.Contains(text, "group: native-metadata-sync\n  cancel-in-progress: false\n  queue: max") {
				t.Fatal("publication and preparation must share a non-cancelling queue")
			}
		}
		header, jobs, ok := strings.Cut(text, "\njobs:\n")
		if !ok || !strings.Contains(header, "\npermissions:\n  contents: read\n") {
			t.Fatalf("%s must default to read-only permissions", name)
		}
		if strings.Contains(header, "id-token:") || strings.Contains(text, "permissions: write-all") || strings.Contains(text, "GITHUB_ENV") {
			t.Fatalf("%s broadens credential access", name)
		}
		for _, marker := range []string{"ANTHROPIC", "steps.anthropic", "secrets.", "GH_TOKEN", "GITHUB_TOKEN", "steps.app.outputs.token"} {
			if strings.Contains(header, marker) {
				t.Fatalf("%s exposes credentials at workflow scope", name)
			}
		}
		// Inspect the block-style jobs and steps; actionlint checks the YAML syntax.
		matches := jobPattern.FindAllStringSubmatchIndex(jobs, -1)
		if len(matches) == 0 {
			t.Fatalf("%s has no jobs", name)
		}
		resolvers := 0
		for i, match := range matches {
			jobName := jobs[match[2]:match[3]]
			end := len(jobs)
			if i+1 < len(matches) {
				end = matches[i+1][0]
			}
			job := jobs[match[1]:end]
			t.Run(name+"/"+jobName, func(t *testing.T) {
				resolver := name == "sync-native-metadata.yml" && slices.Contains([]string{"prepare", "repair-1", "repair-2"}, jobName)
				signer := name == "sync-native-metadata.yml" && slices.Contains([]string{"sign-0", "sign-1", "sign-2", "seal"}, jobName)
				app := name == "sync-native-metadata.yml" && jobName == "publish"
				environment := ""
				switch {
				case resolver:
					resolvers++
					environment = "native-metadata-sync-resolve"
				case signer:
					environment = "native-metadata-sync-sign"
				case app:
					environment = "native-metadata-sync-publish"
				}
				jobHeader, stepText, _ := strings.Cut(job, "\n    steps:")
				jobHeader += "\n"
				if name == "sync-native-metadata.yml" && jobName != "result" || name == "validate-native-metadata-sync.yml" {
					if !strings.Contains(jobHeader, "!cancelled()") {
						t.Fatal("pipeline job ignores cancellation")
					}
					if jobName != "publish" && !strings.Contains(jobHeader, "github.run_attempt == 1") {
						t.Fatal("pipeline job permits spending or rebuilding on a rerun")
					}
				}
				if app {
					guard := strings.Index(job, "- name: Require exact sealed input")
					download := strings.Index(job, "- uses: actions/download-artifact@")
					mint := strings.Index(job, "- uses: grafana/shared-workflows/actions/create-github-app-token@")
					if guard < 0 || download <= guard || mint <= download {
						t.Fatal("publisher must validate and download input before minting credentials")
					}
					for _, input := range []string{"artifact-ids: ${{ needs.seal.outputs.artifact }}", "github-token: ${{ github.token }}", "run-id: ${{ github.run_id }}", "repository: ${{ github.repository }}"} {
						if !strings.Contains(job[download:mint], input) {
							t.Fatalf("missing publisher artifact binding: %s", input)
						}
					}
				}
				if environment == "" {
					if strings.Contains(job, "environment:") {
						t.Fatal("unprivileged job has an environment")
					}
				} else if !strings.Contains(jobHeader, "    environment: "+environment+"\n") {
					t.Fatalf("job must use %s", environment)
				}
				writes := 0
				if resolver || app {
					writes = 1
					if !strings.Contains(jobHeader, "    permissions:\n") || !strings.Contains(jobHeader, "      contents: read\n") || !strings.Contains(jobHeader, "      id-token: write\n") {
						t.Fatal("Vault access needs explicit job-level contents read and OIDC write permissions")
					}
				} else if strings.Contains(job, "id-token:") {
					t.Fatal("unprivileged job requests OIDC")
				}
				if strings.Count(job, ": write") != writes || (!signer && strings.Contains(job, "secrets.")) {
					t.Fatal("job has unexpected write permissions or secrets")
				}
				if strings.Contains(job, "create-github-app-token@") != app {
					t.Fatal("App tokens must stay in publish-environment jobs")
				}
				actionsRead := (name == "sync-native-metadata.yml" && jobName == "seal") || app
				if strings.Contains(job, "actions:") != actionsRead || (actionsRead && !strings.Contains(jobHeader, "      actions: read\n")) {
					t.Fatal("Actions reads must stay in trusted preparation and publication jobs")
				}
				if strings.Contains(jobHeader, "GH_TOKEN") || strings.Contains(jobHeader, "GITHUB_TOKEN") || strings.Contains(jobHeader, "steps.app.outputs.token") || (!app && strings.Contains(job, "steps.app.outputs.token")) {
					t.Fatal("publication tokens escape their controller step")
				}
				for _, marker := range []string{"ANTHROPIC", "steps.anthropic", "get-vault-secrets@"} {
					if strings.Contains(jobHeader, marker) || (!resolver && strings.Contains(job, marker)) {
						t.Fatal("Anthropic credentials escape resolver steps")
					}
				}
				steps := strings.Split(stepText, "\n      - ")[1:]
				for i := range steps {
					steps[i] += "\n"
				}
				if app {
					if permissionPattern.FindString(jobHeader) != "    permissions:\n      contents: read\n      actions: read\n      id-token: write\n" {
						t.Fatal("publication needs only contents read, Actions read, and OIDC write")
					}
					runs := 0
					for _, step := range steps {
						if strings.Contains(step, "\n        id: run\n") {
							runs++
							if !strings.Contains(step, "        env:\n") || !strings.Contains(step, "          GH_TOKEN: ${{ steps.app.outputs.token }}\n") || !strings.Contains(step, "          GITHUB_TOKEN: ${{ github.token }}\n") {
								t.Fatal("publication must receive distinct App and validation tokens")
							}
						} else if strings.Contains(step, "GH_TOKEN") || strings.Contains(step, "GITHUB_TOKEN") || strings.Contains(step, "steps.app.outputs.token") {
							t.Fatal("unrelated step receives publication tokens")
						}
					}
					if runs != 1 {
						t.Fatal("expected one publication controller step")
					}
				}
				if !resolver {
					return
				}
				build, vault, run := -1, -1, -1
				for i, step := range steps {
					switch {
					case strings.Contains(step, "uses: ./.github/actions/native-metadata-sync\n"):
						build = i
					case strings.Contains(step, "\n        id: anthropic\n"):
						if vault != -1 {
							t.Fatal("duplicate Vault fetch")
						}
						vault = i
					case strings.Contains(step, "\n        id: run\n"):
						run = i
					}
					if i != vault && i != run {
						if strings.Contains(step, "ANTHROPIC") || strings.Contains(step, "steps.anthropic") || strings.Contains(step, "get-vault-secrets@") {
							t.Fatal("unrelated step accesses Anthropic credentials")
						}
					}
					if strings.Contains(step, "actions/download-artifact@") && vault != -1 {
						t.Fatal("inputs must be downloaded before fetching credentials")
					}
				}
				if build < 0 || vault <= build || run != vault+1 {
					t.Fatal("expected controller build, Vault fetch, then immediate resolver invocation")
				}
				for _, required := range []string{
					"uses: grafana/shared-workflows/actions/get-vault-secrets@e46fe1e9a2bf9e618bcf8d8d32f3a7381b45c06d",
					"          vault_instance: ops\n",
					"          ignore_missing: 'false'\n",
					"          repo_secrets: |\n            ANTHROPIC_API_KEY=native-metadata-sync-anthropic-key:ANTHROPIC_API_KEY\n",
				} {
					if !strings.Contains(steps[vault], required) {
						t.Fatalf("Vault step missing %q", required)
					}
				}
				if strings.Contains(job, "continue-on-error:") || strings.Count(job, "steps.anthropic.outputs.secrets") != 1 || !strings.Contains(steps[run], "        env:\n") || !strings.Contains(steps[run], "          ANTHROPIC_API_KEY: ${{ fromJSON(steps.anthropic.outputs.secrets).ANTHROPIC_API_KEY }}\n") {
					t.Fatal("resolver must fail closed and receive the Vault output only through its environment")
				}
				_, runBlock, ok := strings.Cut(steps[run], "\n        run: |\n")
				if !ok {
					t.Fatal("missing resolver script")
				}
				var script strings.Builder
				for line := range strings.SplitSeq(strings.TrimRight(runBlock, "\n"), "\n") {
					if !strings.HasPrefix(line, "          ") {
						t.Fatal("unexpected resolver script indentation")
					}
					script.WriteString(strings.TrimPrefix(line, "          ") + "\n")
				}
				for _, tc := range []struct {
					name string
					key  string
				}{
					{name: "unset"},
					{name: "empty"},
					{name: "present", key: "dummy-api-key"},
					{name: "literal shell characters", key: "dummy-$(printf expanded)-`printf expanded`-$HOME-';\""},
				} {
					t.Run(tc.name, func(t *testing.T) {
						dir := t.TempDir()
						write(t, dir, "native-metadata-sync", "#!/bin/bash\n[[ \"$ANTHROPIC_API_KEY\" == \"$EXPECTED_KEY\" ]] || exit 2\nprintf '%s\\n' \"$@\" > \"$RUNNER_TEMP/invocation\"\n")
						must(t, os.Chmod(filepath.Join(dir, "native-metadata-sync"), 0o700))
						cmd := exec.CommandContext(t.Context(), "bash", "-e", "-c", script.String())
						cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "RUNNER_TEMP=" + dir, "ENVELOPE=dummy envelope", "EXPECTED_KEY=" + tc.key}
						if tc.name != "unset" {
							cmd.Env = append(cmd.Env, "ANTHROPIC_API_KEY="+tc.key)
						}
						output, err := cmd.CombinedOutput()
						invocation, readErr := os.ReadFile(filepath.Join(dir, "invocation"))
						if tc.key == "" {
							if err == nil || !errors.Is(readErr, os.ErrNotExist) || string(output) != "::error::ANTHROPIC_API_KEY is empty\n" {
								t.Fatalf("empty key did not stop before invocation: %v, %v, %q", err, readErr, output)
							}
							return
						}
						must(t, err)
						must(t, readErr)
						wantArgs := []string{"prepare", "--work", filepath.Join(dir, "candidate"), "--out", "output"}
						if jobName != "prepare" {
							wantArgs = []string{"repair", "--work", filepath.Join(dir, "candidate"), "--input", "input", "--envelope", "dummy envelope", "--diagnostics", "diagnostics", "--out", "output"}
						}
						if len(output) != 0 || string(invocation) != strings.Join(wantArgs, "\n")+"\n" {
							t.Fatalf("unexpected resolver output %q or arguments %q", output, invocation)
						}
					})
				}
			})
		}
		if name == "sync-native-metadata.yml" && resolvers != 3 {
			t.Fatalf("expected three resolver jobs, got %d", resolvers)
		}
	}
}

func TestBlockFilesAreEditableButDependencyManifestsAreNot(t *testing.T) {
	for _, name := range []string{"tsdb/block.go", "tsdb/block_test.go", "tsdb/blockwriter.go"} {
		if !editable(name) {
			t.Fatalf("valid source path rejected: %s", name)
		}
	}
	for _, name := range []string{"go.mod", "go.work", "go.work.sum", "web/ui/package.json", "web/ui/pnpm-lock.yaml", "web/ui/.npmrc", "model/test.pb.go"} {
		if editable(name) {
			t.Fatalf("dependency or generated file accepted: %s", name)
		}
	}
}

func TestHumanAutomationChangeStopsReplay(t *testing.T) {
	g, previous := fixture(t)
	m := previous
	m.Target = commit(t, g, m.Target, "human automation edit", map[string]string{".github/workflows/new.yml": "unreviewed workflow\n"})
	if g.replay(t.Context(), previous, &m, nil) == nil {
		t.Fatal("unreviewed automation was copied to a snapshot ref")
	}
}

func TestRepairCarriesLedgerAndStopsAfterTwoRounds(t *testing.T) {
	g, m := fixture(t)
	previous := m
	m.SourceHead = commit(t, g, m.SourceHead, "code", map[string]string{"code.go": "package example\nvar value = 0\n"})
	must(t, g.replay(t.Context(), previous, &m, nil))
	signing(t, &g)
	_, err := g.signCandidate(t.Context(), &m)
	must(t, err)
	diagnostics := t.TempDir()
	write(t, diagnostics, "go.log", "code.go:2: unexpected value\n")
	calls := 0
	resolver := resolverFunc(func(_ context.Context, r resolveRequest, l *ledger) (resolution, error) {
		if len(l.Charges) != calls {
			t.Fatal("repair lost earlier charges")
		}
		_, e := l.reserve()
		if e != nil {
			return resolution{}, e
		}
		calls++
		for _, f := range r.Files {
			if f.Path == "code.go" {
				return resolution{Summary: "Repair the reported behavior.", Edits: []fileEdit{{Path: f.Path, Before: f.SHA256, Replacements: []replacement{{Old: fmt.Sprintf("value = %d", calls-1), New: fmt.Sprintf("value = %d", calls)}}}}}, nil
			}
		}
		return resolution{}, errors.New("caller missing")
	})
	for round := 1; round <= 2; round++ {
		must(t, g.repair(t.Context(), &m, resolver, diagnostics))
		if m.Round != round || len(m.Ledger.Charges) != round {
			t.Fatal("repair did not advance round and ledger together")
		}
		_, err = g.signCandidate(t.Context(), &m)
		must(t, err)
	}
	if g.repair(t.Context(), &m, resolver, diagnostics) == nil || calls != 2 {
		t.Fatal("repair exceeded the two-round limit")
	}
}

func TestAmbiguousDeltaRequiresExplicitReconciliation(t *testing.T) {
	g, previous := fixture(t)
	first := previous.Mapping[0]
	reverted := commit(t, g, first.Source, "revert", map[string]string{"a.txt": "base\n"})
	again := commit(t, g, reverted, "feature again", map[string]string{"a.txt": "source\n"})
	localRevert := commit(t, g, first.Local, "projected revert", map[string]string{"a.txt": "base\n"})
	localAgain := commit(t, g, localRevert, "projected feature again", map[string]string{"a.txt": "source\n", "fork.txt": "second adaptation\n"})
	overlay := commit(t, g, localAgain, "overlay", map[string]string{"overlay.txt": "local overlay\n"})
	d2, err := g.delta(t.Context(), reverted)
	must(t, err)
	d3, err := g.delta(t.Context(), again)
	must(t, err)
	if first.Delta != d3 {
		t.Fatal("fixture lacks duplicate exact deltas")
	}
	previous.SourceHead = again
	previous.Target = overlay
	previous.Candidate = overlay
	previous.Mapping = append(previous.Mapping, projection{Source: reverted, Local: localRevert, Delta: d2}, projection{Source: again, Local: localAgain, Delta: d3, Adapted: true})
	previous.Overlays = []string{overlay}
	m := previous
	m.SourceHead = commit(t, g, previous.SourceBase, "squashed feature", map[string]string{"a.txt": "source\n"})
	var reconciled []string
	resolver := resolverFunc(func(_ context.Context, r resolveRequest, _ *ledger) (resolution, error) {
		reconciled = append(reconciled, r.Adaptations...)
		source := r.Adaptations[0]
		result := resolution{Summary: "Preserve fork adaptation.", Adaptations: []adaptation{{Source: source, Disposition: "retained", Reason: "Preserve both prior fork additions."}}}
		for _, f := range r.Files {
			if f.Path == "fork.txt" && f.Content != "both adaptations\n" {
				result.Edits = []fileEdit{{Path: f.Path, Before: f.SHA256, Replacements: []replacement{{Old: f.Content, New: "both adaptations\n"}}}}
			}
		}
		return result, nil
	})
	must(t, g.replay(t.Context(), previous, &m, resolver))
	if !slices.Equal(reconciled, []string{first.Source, again}) {
		t.Fatalf("ambiguous delta reused a projection: %v", reconciled)
	}
	signing(t, &g)
	envelope, err := g.signCandidate(t.Context(), &m)
	must(t, err)
	ok, err := g.ancestor(t.Context(), again, envelope)
	must(t, err)
	if !ok {
		t.Fatal("rewritten previous source history was not retained in the checkpoint")
	}
}

func TestMissingAdaptationDispositionFails(t *testing.T) {
	for _, stored := range []bool{true, false} {
		t.Run(fmt.Sprintf("stored=%v", stored), func(t *testing.T) {
			g, previous := fixture(t)
			previous.Mapping[0].Adapted = stored
			m := previous
			m.SourceHead = commit(t, g, previous.SourceBase, "rewritten feature", map[string]string{"a.txt": "changed source\n"})
			resolver := resolverFunc(func(context.Context, resolveRequest, *ledger) (resolution, error) {
				return resolution{Summary: "No explicit disposition."}, nil
			})
			if g.replay(t.Context(), previous, &m, resolver) == nil {
				t.Fatal("adaptation silently dropped")
			}
		})
	}
}

func TestWrongSigningKeyFails(t *testing.T) {
	g, m := fixture(t)
	signing(t, &g)
	envelope, err := g.signCandidate(t.Context(), &m)
	must(t, err)
	signing(t, &g)
	if _, err = g.loadManifest(t.Context(), envelope, true); err == nil {
		t.Fatal("checkpoint accepted from an untrusted signing key")
	}
}

func TestIncompleteNormalizationStillRequiresGeneratedValidation(t *testing.T) {
	g, m := fixture(t)
	next := commit(t, g, m.Candidate, "tracked generated file", map[string]string{"go.sum": "before\n"})
	m.Overlays = append(m.Overlays, next)
	m.Candidate = next
	bin := t.TempDir()
	write(t, bin, "go", "#!/bin/sh\nprintf 'after\\n' > go.sum\nprintf 'code.go:2: compile failure\\n' >&2\nexit 1\n")
	must(t, os.Chmod(filepath.Join(bin, "go"), 0o755))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	out := t.TempDir()
	must(t, g.normalize(t.Context(), out))
	git(t, g, "reset", "--hard", m.Candidate)
	must(t, g.applyNormalization(t.Context(), &m, filepath.Join(out, "normalization.patch")))
	signing(t, &g)
	_, err := g.signCandidate(t.Context(), &m)
	must(t, err)
	if err = g.validate(t.Context(), m, "generated", filepath.Join(t.TempDir(), "generated.log")); err == nil {
		t.Fatal("incomplete generation incorrectly passed the authoritative check")
	}
}

func TestClaudeStreamingResponse(t *testing.T) {
	events := []map[string]any{
		{"type": "message_start", "message": map[string]any{"content": []any{}, "usage": map[string]int{"input_tokens": 120, "output_tokens": 1}}},
		{"type": "content_block_start", "index": 0, "content_block": map[string]string{"type": "thinking"}},
		{"type": "content_block_delta", "index": 0, "delta": map[string]string{"type": "thinking_delta", "thinking": "not JSON"}},
		{"type": "content_block_stop", "index": 0},
		{"type": "content_block_start", "index": 1, "content_block": map[string]string{"type": "text", "text": ""}},
		{"type": "content_block_delta", "index": 1, "delta": map[string]string{"type": "text_delta", "text": "{\"edits\":[],"}},
		{"type": "content_block_delta", "index": 1, "delta": map[string]string{"type": "text_delta", "text": "\"summary\":\"Done.\",\"adaptations\":[]}"}},
		{"type": "content_block_stop", "index": 1},
		{"type": "message_delta", "delta": map[string]string{"stop_reason": "end_turn"}, "usage": map[string]int{"output_tokens": 99}},
		{"type": "message_stop"},
	}
	for _, mode := range []string{"complete", "truncated", "error", "invalid index"} {
		t.Run(mode, func(t *testing.T) {
			list := slices.Clone(events)
			switch mode {
			case "truncated":
				list = list[:len(list)-1]
			case "error":
				list[len(list)-1] = map[string]any{"type": "error"}
			case "invalid index":
				list[1] = map[string]any{"type": "content_block_start", "index": 3}
			}
			var stream strings.Builder
			for _, event := range list {
				data, _ := json.Marshal(event)
				fmt.Fprintf(&stream, "event: %s\ndata: %s\n\n", event["type"], data)
			}
			resp := &http.Response{Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(stream.String()))}
			result, err := readMessage(resp)
			if (err == nil) != (mode == "complete") {
				t.Fatalf("unexpected stream result: %v", err)
			}
			if err == nil {
				if result.Stop != "end_turn" || result.Usage.Input != 120 || result.Usage.Output != 99 {
					t.Fatal("stream lost final usage")
				}
				var resolved resolution
				must(t, decodeJSON([]byte(result.Content[1].Text), &resolved))
			}
		})
	}
}

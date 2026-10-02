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
	"errors"
	"fmt"
	"go/format"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

var generatedPaths = []string{
	"go.mod", "go.sum", "go.work.sum",
	"docs/command-line/prometheus.md", "docs/command-line/promtool.md",
	"cmd/prometheus/testdata/features.json",
	"web/api/v1/testdata/openapi_3.1_golden.yaml",
	"web/api/v1/testdata/openapi_3.2_golden.yaml",
}

// These commands run exclusively in jobs without resolver, signing, or write credentials.
var generators = [][]string{
	{"go", "mod", "tidy"},
	{"make", "cli-documentation"},
	{"make", "update-features-testdata"},
	{"go", "test", "./web/api/v1", "-run", "^TestOpenAPIGolden_", "-update-openapi-spec"},
}

var validations = map[string][][]string{
	"go": {
		{"make", "GO_ONLY=1", "SKIP_GOLANGCI_LINT=1"},
		{"go", "test", "./tsdb/", "-test.tsdb-isolation=false"},
		{"make", "-C", "documentation/examples/remote_storage"},
		{"make", "-C", "documentation/examples"},
	},
	"variants": {
		{"go", "test", "--tags=dedupelabels", "./..."},
		{"go", "test", "--tags=slicelabels", "-race", "./cmd/prometheus", "./cmd/promtool/...", "./model/textparse", "./prompb/..."},
		{"go", "test", "--tags=forcedirectio", "-race", "./tsdb/"},
	},
	"lint": {
		{"make", "lint"},
	},
	"generated": {
		{"make", "protoc"},
		{"make", "proto"},
	},
	"ui": {
		{"make", "ui-install"},
		{"make", "assets"},
		{"make", "ui-lint"},
		{"make", "ui-test"},
	},
}

type tailWriter struct{ b []byte }

func (w *tailWriter) Write(p []byte) (int, error) {
	n := len(p)
	w.b = append(w.b, p...)
	if len(w.b) > 256<<10 {
		w.b = bytes.Clone(w.b[len(w.b)-(256<<10):])
	}
	return n, nil
}

func runCommands(ctx context.Context, dir string, commands [][]string, diagnostics string) error {
	tail := &tailWriter{}
	for _, args := range commands {
		fmt.Fprintf(tail, "$ %s\n", strings.Join(args, " "))
		cmd := exec.CommandContext(ctx, args[0], args[1:]...)
		cmd.Dir = dir
		cmd.Env = commandEnv("GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0", "CI=true")
		cmd.Stdout = io.MultiWriter(os.Stdout, tail)
		cmd.Stderr = io.MultiWriter(os.Stderr, tail)
		err := cmd.Run()
		if diagnostics != "" {
			if e := os.MkdirAll(filepath.Dir(diagnostics), 0o700); e != nil {
				return e
			}
			if e := os.WriteFile(diagnostics, tail.b, 0o600); e != nil {
				return e
			}
		}
		if err != nil {
			return fmt.Errorf("%s: %w", strings.Join(args, " "), err)
		}
	}
	return nil
}

func (g gitRepo) formatGo(ctx context.Context, m *manifest) error {
	paths, err := g.changed(ctx, m.Main, m.Candidate)
	if err != nil {
		return err
	}
	for _, p := range paths {
		if !strings.HasSuffix(p, ".go") || !editable(p) {
			continue
		}
		b, e := readRegular(g.dir, p, true)
		if e != nil {
			return e
		}
		if b == nil {
			continue
		}
		formatted, e := format.Source(b)
		if e != nil {
			return fmt.Errorf("format %s: %w", p, e)
		}
		if bytes.Equal(b, formatted) {
			continue
		}
		if e = os.WriteFile(filepath.Join(g.dir, p), formatted, 0o644); e != nil {
			return e
		}
		if _, e = g.git(ctx, "add", "--", p); e != nil {
			return e
		}
	}
	return g.integrate(ctx, m, "chore: format integrated Go sources")
}

func (g gitRepo) normalize(ctx context.Context, out string) error {
	// Generation may need the same compilation repair as the candidate itself.
	// Preserve its partial allowed changes; the mandatory generated check reruns
	// every generator after signing and is the authoritative success gate.
	if err := runCommands(ctx, g.dir, generators, filepath.Join(out, "normalization.log")); err != nil {
		fmt.Fprintln(os.Stderr, "Normalization incomplete; final generated validation must succeed:", err)
	}
	// Include newly created generated files in the patch, respecting .gitignore.
	untracked, err := g.git(ctx, append([]string{"ls-files", "--others", "--exclude-standard", "-z", "--"}, generatedPaths...)...)
	if err != nil {
		return err
	}
	if untracked != "" {
		paths := strings.Split(strings.TrimSuffix(untracked, "\x00"), "\x00")
		if _, err = g.git(ctx, append([]string{"add", "--intent-to-add", "--"}, paths...)...); err != nil {
			return err
		}
	}
	patch, err := g.command(ctx, nil, nil, "diff", "--binary", "--no-ext-diff", "--no-textconv", "HEAD")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "normalization.patch"), patch, 0o600)
}

func (g gitRepo) applyNormalization(ctx context.Context, m *manifest, patchFile string) error {
	b, err := os.ReadFile(patchFile)
	if err != nil {
		return err
	}
	if len(b) > 4<<20 {
		return errors.New("normalization patch exceeds 4 MiB")
	}
	if len(b) == 0 {
		return nil
	}
	if _, err = g.command(ctx, b, nil, "apply", "--cached", "--whitespace=error", "-"); err != nil {
		return err
	}
	tree, err := g.git(ctx, "write-tree")
	if err != nil {
		return err
	}
	names, err := g.policyChanges(ctx, m.Candidate, tree)
	if err != nil {
		return err
	}
	for _, p := range names {
		if !slices.Contains(generatedPaths, p) {
			return fmt.Errorf("normalization changed unexpected path %q", p)
		}
		mode, e := g.git(ctx, "ls-tree", tree, "--", p)
		if e != nil {
			return e
		}
		if !strings.HasPrefix(mode, "100644 blob ") {
			return errors.New("normalization must produce regular generated files")
		}
	}
	if err = g.integrate(ctx, m, "chore: regenerate integrated metadata artifacts"); err != nil {
		return err
	}
	// Checkout of a new tree from a cached patch must refresh the worktree too.
	_, err = g.git(ctx, "read-tree", "--reset", "-u", m.Candidate)
	return err
}

func (g gitRepo) validate(ctx context.Context, m manifest, check, diagnostics string) error {
	if err := g.verifyCandidate(ctx, m); err != nil {
		return err
	}
	commands, ok := validations[check]
	if !ok {
		return errors.New("unknown validation check")
	}
	if check == "generated" {
		commands = append(slices.Clone(generators), commands...)
	}
	if err := runCommands(ctx, g.dir, commands, diagnostics); err != nil {
		return err
	}
	// Tracked changes and new, non-ignored files must not escape the signed candidate.
	status, err := g.git(ctx, "status", "--porcelain", "--untracked-files=all")
	if err != nil {
		return err
	}
	if status != "" {
		err = fmt.Errorf("validation changed candidate files:\n%s", status)
		if diagnostics != "" {
			previous, e := os.ReadFile(diagnostics)
			if e != nil {
				return errors.Join(err, e)
			}
			tail := &tailWriter{b: previous}
			// Keep the failure summary even when command output or status is large.
			fmt.Fprintf(tail, "\n%s\nvalidation changed candidate files\n", status)
			if e = os.WriteFile(diagnostics, tail.b, 0o600); e != nil {
				return errors.Join(err, e)
			}
		}
		return err
	}
	return nil
}

var diagnosticFile = regexp.MustCompile(`(?:^|[\s(])([a-zA-Z0-9_./-]+\.go):[0-9]+`)

func (g gitRepo) repair(ctx context.Context, m *manifest, r resolver, diagnosticsDir string) error {
	if m.Round >= 2 {
		return errors.New("repair rounds exhausted")
	}
	if err := g.verifyCandidate(ctx, *m); err != nil {
		return err
	}
	paths, err := g.changed(ctx, m.Main, m.Candidate)
	if err != nil {
		return err
	}
	changed := paths
	paths = nil
	tracked, err := g.git(ctx, "ls-tree", "-r", "--name-only", m.Candidate)
	if err != nil {
		return err
	}
	trackedFiles := strings.Split(tracked, "\n")
	var evidence strings.Builder
	entries, err := os.ReadDir(diagnosticsDir)
	if err != nil {
		return err
	}
	if len(entries) == 0 || len(entries) > len(checks) {
		return errors.New("unexpected diagnostic artifacts")
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".log") {
			return errors.New("unexpected diagnostic file")
		}
		b, e := os.ReadFile(filepath.Join(diagnosticsDir, entry.Name()))
		if e != nil {
			return e
		}
		if len(b) > 256<<10 {
			return errors.New("diagnostic log exceeds 256 KiB")
		}
		evidence.WriteString(entry.Name() + "\n")
		evidence.Write(b)
		for _, match := range diagnosticFile.FindAllSubmatch(b, -1) {
			name := strings.TrimPrefix(strings.TrimPrefix(string(match[1]), g.dir+"/"), "./")
			// Go test often prints package-relative filenames. Require a unique
			// tracked match before treating one as an editable caller.
			if !slices.Contains(trackedFiles, name) {
				matches := []string{}
				for _, file := range trackedFiles {
					if strings.HasSuffix(file, "/"+name) {
						matches = append(matches, file)
					}
				}
				if len(matches) == 1 {
					name = matches[0]
				}
			}
			if editable(name) {
				if _, e = readRegular(g.dir, name, false); e == nil {
					paths = append(paths, name)
				}
			}
		}
	}
	// Keep context bounded to changed files and compiler-reported callers. A very
	// broad failure stops instead of handing the model unrestricted repository edits.
	slices.Sort(paths)
	paths = slices.Compact(paths)
	if len(paths) == 0 {
		for _, p := range changed {
			if editable(p) && (strings.HasSuffix(p, ".go") || strings.HasSuffix(p, ".ts") || strings.HasSuffix(p, ".tsx")) {
				paths = append(paths, p)
			}
		}
	}
	files, err := g.scope(ctx, paths)
	if err != nil {
		return err
	}
	result, err := r.resolve(ctx, resolveRequest{Task: "Repair validation failures. Preserve tests and fork contracts.", Files: files, Evidence: evidence.String()}, &m.Ledger)
	if err != nil {
		return err
	}
	if len(result.Adaptations) != 0 {
		return errors.New("unexpected adaptation report in repair")
	}
	names, err := applyResolution(g.dir, files, result)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return errors.New("repair did not produce any edits")
	}
	if _, err = g.git(ctx, append([]string{"add", "--"}, names...)...); err != nil {
		return err
	}
	m.Notes = append(m.Notes, result.Summary)
	m.Round++
	if err = g.integrate(ctx, m, "tsdb: repair native metadata integration"); err != nil {
		return err
	}
	if err = g.formatGo(ctx, m); err != nil {
		return err
	}
	return g.checkPolicy(ctx, m.Main, m.Candidate)
}

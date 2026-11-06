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
	"fmt"
	"path"
	"slices"
	"strings"
)

type resolver interface {
	resolve(context.Context, resolveRequest, *ledger) (resolution, error)
}

func (g gitRepo) checkMapping(ctx context.Context, m manifest) error {
	source, err := g.linear(ctx, m.SourceBase, m.SourceHead)
	if err != nil {
		return err
	}
	local, err := g.linear(ctx, m.Main, m.Candidate)
	if err != nil {
		return err
	}
	if len(source) != len(m.Mapping) || len(local) != len(m.Mapping)+len(m.Overlays) {
		return errors.New("mapping does not cover source and local history exactly")
	}
	for i, p := range m.Mapping {
		d, e := g.delta(ctx, p.Source)
		if e != nil {
			return e
		}
		if p.Source != source[i] || p.Local != local[i] || p.Delta != d {
			return errors.New("projection order or source delta differs from checkpoint")
		}
	}
	if !slices.Equal(local[len(m.Mapping):], m.Overlays) {
		return errors.New("overlay order differs from checkpoint")
	}
	return nil
}

func (g gitRepo) scope(ctx context.Context, paths []string) ([]contextFile, error) {
	all, err := g.git(ctx, "ls-files", "-z")
	if err != nil {
		return nil, err
	}
	files, err := collectFiles(g.dir, paths)
	if err != nil {
		return nil, err
	}
	selected, dirs := map[string]bool{}, map[string]bool{}
	size := 0
	var related []string
	for _, f := range files {
		selected[f.Path] = true
		dirs[path.Dir(f.Path)] = true
		size += len(f.Content)
		if strings.HasSuffix(f.Path, ".go") && !strings.HasSuffix(f.Path, "_test.go") {
			related = append(related, strings.TrimSuffix(f.Path, ".go")+"_test.go", strings.TrimSuffix(f.Path, ".go")+"_sync_test.go")
		}
	}
	if size > 1<<20 || len(files) > 80 {
		return nil, errors.New("required resolution files exceed context limit")
	}
	// Include adjacent tests first, then as much package context as fits. Large
	// packages must not make every ordinary conflict exceed the context limit.
	for p := range strings.SplitSeq(all, "\x00") {
		if dirs[path.Dir(p)] && strings.HasSuffix(p, ".go") {
			related = append(related, p)
		}
	}
	for _, p := range related {
		if selected[p] || !editable(p) || len(files) >= 80 {
			continue
		}
		data, e := collectFiles(g.dir, []string{p})
		if e != nil || size+len(data[0].Content) > 1<<20 {
			continue
		}
		selected[p] = true
		size += len(data[0].Content)
		files = append(files, data...)
	}
	return files, nil
}

func (g gitRepo) resolveFiles(ctx context.Context, m *manifest, r resolver, task, evidence string, paths, adaptations []string) error {
	if r == nil {
		return errors.New("manual conflict resolution is required")
	}
	files, err := g.scope(ctx, paths)
	if err != nil {
		return err
	}
	result, err := r.resolve(ctx, resolveRequest{Task: task, Files: files, Evidence: evidence, Adaptations: adaptations}, &m.Ledger)
	if err != nil {
		return err
	}
	want := map[string]bool{}
	for _, s := range adaptations {
		want[s] = true
	}
	for _, a := range result.Adaptations {
		if !want[a.Source] || !slices.Contains([]string{"retained", "superseded", "removed"}, a.Disposition) || strings.TrimSpace(a.Reason) == "" {
			return errors.New("invalid adaptation disposition")
		}
		delete(want, a.Source)
	}
	if len(want) > 0 {
		return errors.New("not every prior adaptation has been reconciled")
	}
	names, err := applyResolution(g.dir, files, result)
	if err != nil {
		return err
	}
	if len(names) > 0 {
		if _, err = g.git(ctx, append([]string{"add", "--"}, names...)...); err != nil {
			return err
		}
	}
	m.Notes = append(m.Notes, result.Summary)
	m.Adaptations = append(m.Adaptations, result.Adaptations...)
	return nil
}

func (g gitRepo) applyCommit(ctx context.Context, original, metadata, parent string, m *manifest, r resolver) (string, error) {
	if _, err := g.git(ctx, "checkout", "--quiet", "--detach", parent); err != nil {
		return "", err
	}
	_, applyErr := g.git(ctx, "cherry-pick", "--no-commit", original)
	conflicts, err := g.git(ctx, "diff", "--name-only", "--diff-filter=U", "-z")
	if err != nil {
		return "", err
	}
	if applyErr != nil && conflicts == "" {
		return "", applyErr
	}
	if conflicts != "" {
		paths := strings.Split(strings.TrimSuffix(conflicts, "\x00"), "\x00")
		for _, p := range paths {
			stages, e := g.git(ctx, "ls-files", "--unmerged", "--", p)
			if e != nil {
				return "", e
			}
			lines := strings.Split(stages, "\n")
			// Renames, additions/deletions, symlinks and gitlinks need manual integration.
			if len(lines) != 3 {
				return "", errors.New("only regular modify/modify conflicts are supported")
			}
			for _, line := range lines {
				if !strings.HasPrefix(line, "100644 ") && !strings.HasPrefix(line, "100755 ") {
					return "", errors.New("non-regular conflict")
				}
			}
		}
		patch, e := g.git(ctx, "show", "--format=fuller", "--no-ext-diff", "--no-textconv", original)
		if e != nil {
			return "", e
		}
		if err = g.resolveFiles(ctx, m, r, "Resolve this replay conflict, preserving both fork and source behavior.", patch, paths, nil); err != nil {
			return "", err
		}
		left, e := g.git(ctx, "diff", "--name-only", "--diff-filter=U")
		if e != nil {
			return "", e
		}
		if left != "" {
			return "", errors.New("unresolved replay conflict")
		}
	}
	tree, err := g.git(ctx, "write-tree")
	if err != nil {
		return "", err
	}
	meta, err := g.git(ctx, "show", "-s", "--format=%an%x00%ae%x00%aI%x00%B", metadata)
	if err != nil {
		return "", err
	}
	fields := strings.SplitN(meta, "\x00", 4)
	if len(fields) != 4 {
		return "", errors.New("invalid replay commit metadata")
	}
	oid, err := g.commitTree(ctx, tree, []string{parent}, fields[3], []string{"GIT_AUTHOR_NAME=" + fields[0], "GIT_AUTHOR_EMAIL=" + fields[1], "GIT_AUTHOR_DATE=" + fields[2]}, m.Created, false)
	if err != nil {
		return "", err
	}
	// Clear cherry-pick metadata without resetting the resulting tree.
	if _, err = g.git(ctx, "cherry-pick", "--quit"); err != nil {
		return "", err
	}
	_, err = g.git(ctx, "checkout", "--quiet", "--detach", oid)
	return oid, err
}

func (g gitRepo) integrate(ctx context.Context, m *manifest, message string) error {
	tree, err := g.git(ctx, "write-tree")
	if err != nil {
		return err
	}
	oldTree, err := g.git(ctx, "rev-parse", m.Candidate+"^{tree}")
	if err != nil {
		return err
	}
	if tree == oldTree {
		return nil
	}
	oid, err := g.commitTree(ctx, tree, []string{m.Candidate}, message, nil, m.Created, false)
	if err != nil {
		return err
	}
	m.Candidate = oid
	m.Overlays = append(m.Overlays, oid)
	_, err = g.git(ctx, "checkout", "--quiet", "--detach", oid)
	return err
}

func (g gitRepo) replay(ctx context.Context, previous manifest, m *manifest, r resolver) error {
	m.PreviousSourceHead = previous.SourceHead
	if err := g.checkMapping(ctx, previous); err != nil {
		return err
	}
	if err := g.checkPolicy(ctx, previous.Main, m.Target); err != nil {
		return err
	}
	human, err := g.linear(ctx, previous.Candidate, m.Target)
	if err != nil {
		return fmt.Errorf("target diverged from the applied checkpoint: %w", err)
	}
	sources, err := g.linear(ctx, m.SourceBase, m.SourceHead)
	if err != nil {
		return err
	}
	oldByID := map[string]projection{}
	oldByDelta := map[string][]projection{}
	for _, p := range previous.Mapping {
		localDelta, e := g.delta(ctx, p.Local)
		if e != nil {
			return e
		}
		// Correct flags on copies, without changing the signed checkpoint.
		p.Adapted = p.Adapted || localDelta != p.Delta
		oldByID[p.Source] = p
		oldByDelta[p.Delta] = append(oldByDelta[p.Delta], p)
	}
	deltas := map[string]string{}
	counts := map[string]int{}
	for _, s := range sources {
		d, e := g.delta(ctx, s)
		if e != nil {
			return e
		}
		deltas[s] = d
		counts[d]++
	}
	m.Mapping = nil
	m.Overlays = nil
	m.Adaptations = nil
	m.Candidate = m.Main
	reused := map[string]bool{}
	for _, s := range sources {
		original := s
		old, ok := oldByID[s]
		if !ok && counts[deltas[s]] == 1 && len(oldByDelta[deltas[s]]) == 1 {
			old = oldByDelta[deltas[s]][0]
			ok = true
		}
		if ok {
			original = old.Local
			reused[old.Source] = true
		}
		local, e := g.applyCommit(ctx, original, s, m.Candidate, m, r)
		if e != nil {
			return e
		}
		localDelta, e := g.delta(ctx, local)
		if e != nil {
			return e
		}
		adapted := localDelta != deltas[s] || ok && old.Adapted
		m.Mapping = append(m.Mapping, projection{Source: s, Local: local, Delta: deltas[s], Adapted: adapted})
		m.Candidate = local
	}
	for _, overlay := range append(slices.Clone(previous.Overlays), human...) {
		local, e := g.applyCommit(ctx, overlay, overlay, m.Candidate, m, r)
		if e != nil {
			return e
		}
		m.Overlays = append(m.Overlays, local)
		m.Candidate = local
	}
	// Exact deltas deliberately do not treat patch IDs as semantic equivalence.
	// Rewritten/squashed adaptations require an explicit disposition even when
	// cherry-pick completed without conflicts.
	for _, p := range previous.Mapping {
		p = oldByID[p.Source]
		if !p.Adapted || reused[p.Source] {
			continue
		}
		sourcePatch, e := g.git(ctx, "show", "--format=fuller", "--no-ext-diff", "--no-textconv", p.Source)
		if e != nil {
			return e
		}
		localPatch, e := g.git(ctx, "show", "--format=fuller", "--no-ext-diff", "--no-textconv", p.Local)
		if e != nil {
			return e
		}
		paths, e := g.changed(ctx, p.Local+"^", p.Local)
		if e != nil {
			return e
		}
		// Removed files are evidence, but cannot be resurrected without manual review.
		tracked, e := g.git(ctx, "ls-tree", "-r", "--name-only", m.Candidate)
		if e != nil {
			return e
		}
		existing := strings.Split(tracked, "\n")
		paths = slices.DeleteFunc(paths, func(p string) bool { return !slices.Contains(existing, p) })
		if e = g.resolveFiles(ctx, m, r, "Reconcile this prior fork adaptation against the new source series.", sourcePatch+"\nFORK PROJECTION:\n"+localPatch, paths, []string{p.Source}); e != nil {
			return e
		}
		if e = g.integrate(ctx, m, "tsdb: preserve fork adaptation after source rewrite"); e != nil {
			return e
		}
	}
	if err = g.checkMapping(ctx, *m); err != nil {
		return err
	}
	return g.checkPolicy(ctx, m.Main, m.Candidate)
}

func (g gitRepo) signCandidate(ctx context.Context, m *manifest) (string, error) {
	if err := g.checkMapping(ctx, *m); err != nil {
		return "", err
	}
	if err := g.checkPolicy(ctx, m.Main, m.Candidate); err != nil {
		return "", err
	}
	commits, err := g.linear(ctx, m.Main, m.Candidate)
	if err != nil {
		return "", err
	}
	mapping := map[string]string{}
	parent := m.Main
	for _, old := range commits {
		next, e := g.copyCommit(ctx, old, parent, m.Created, true)
		if e != nil {
			return "", e
		}
		mapping[old] = next
		parent = next
	}
	for i := range m.Mapping {
		m.Mapping[i].Local = mapping[m.Mapping[i].Local]
	}
	for i := range m.Overlays {
		m.Overlays[i] = mapping[m.Overlays[i]]
	}
	m.Candidate = parent
	parents := []string{m.Candidate, m.SourceHead}
	if m.PreviousSourceHead != "" && !slices.Contains(parents, m.PreviousSourceHead) {
		parents = append(parents, m.PreviousSourceHead)
	}
	if m.PreviousState != "" {
		parents = append(parents, m.PreviousState)
	}
	return g.metadata(ctx, *m, parents)
}

func (g gitRepo) verifyCandidate(ctx context.Context, m manifest) error {
	if err := m.valid(); err != nil {
		return err
	}
	if err := g.checkMapping(ctx, m); err != nil {
		return err
	}
	if err := g.checkPolicy(ctx, m.Main, m.Candidate); err != nil {
		return err
	}
	policy, err := g.policyDigest(ctx, m.Main)
	if err != nil {
		return err
	}
	if policy != m.Policy {
		return errors.New("candidate policy digest mismatch")
	}
	controlPolicy, err := g.policyDigest(ctx, m.Control)
	if err != nil {
		return err
	}
	if controlPolicy != m.Policy {
		return errors.New("control and candidate main have different automation policy")
	}
	commits, err := g.linear(ctx, m.Main, m.Candidate)
	if err != nil {
		return err
	}
	for _, c := range commits {
		if err = g.verify(ctx, c); err != nil {
			return err
		}
		message, e := g.git(ctx, "show", "-s", "--format=%B", c)
		if e != nil {
			return e
		}
		if !slices.Contains(strings.Split(message, "\n"), "Signed-off-by: "+g.name+" <"+g.email+">") {
			return errors.New("candidate commit is missing the bot DCO sign-off")
		}
	}
	return nil
}

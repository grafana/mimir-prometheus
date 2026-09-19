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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

type github struct {
	client          *http.Client
	endpoint, token string
}

func (a github) get(ctx context.Context, route string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.endpoint+"/repos/"+repository+route, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+a.token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("GitHub GET %s: HTTP %d", route, resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(io.LimitReader(resp.Body, 8<<20)).Decode(out)
}

func (a github) jobs(ctx context.Context, m manifest) (*validation, error) {
	if m.Ledger.RunID <= 0 || m.Ledger.Attempt != 1 {
		return nil, errors.New("invalid original run")
	}
	prefix := "/actions/runs/" + strconv.FormatInt(m.Ledger.RunID, 10)
	var run struct {
		ID          int64
		HeadSHA     string `json:"head_sha"`
		HeadBranch  string `json:"head_branch"`
		Path, Event string
		RunAttempt  int `json:"run_attempt"`
		Repository  struct {
			FullName string `json:"full_name"`
		}
	}
	if err := a.get(ctx, prefix+"/attempts/1", &run); err != nil {
		return nil, err
	}
	if run.ID != m.Ledger.RunID || run.HeadSHA != m.Control || run.HeadBranch != "main" || run.RunAttempt != 1 ||
		run.Path != ".github/workflows/sync-native-metadata.yml" || run.Repository.FullName != repository ||
		!slices.Contains([]string{"workflow_dispatch", "schedule"}, run.Event) {
		return nil, errors.New("unexpected validation workflow provenance or rerun")
	}
	found := map[string]int64{}
	for page := 1; page <= 20; page++ {
		var response struct {
			Jobs []struct {
				ID                       int64
				RunID                    int64 `json:"run_id"`
				Name, Status, Conclusion string
				RunAttempt               int    `json:"run_attempt"`
				HeadSHA                  string `json:"head_sha"`
			}
		}
		if err := a.get(ctx, fmt.Sprintf("%s/attempts/1/jobs?per_page=100&page=%d", prefix, page), &response); err != nil {
			return nil, err
		}
		for _, job := range response.Jobs {
			for _, check := range checks {
				name := fmt.Sprintf("validate-%d / native-metadata/%d/%s", m.Round, m.Round, check)
				if job.Name != name {
					continue
				}
				if found[check] != 0 || job.ID <= 0 || job.RunID != m.Ledger.RunID || job.Status != "completed" || job.Conclusion != "success" || job.HeadSHA != m.Control || job.RunAttempt != 1 {
					return nil, fmt.Errorf("required job %q is ambiguous or did not succeed", name)
				}
				found[check] = job.ID
			}
		}
		if len(response.Jobs) < 100 {
			break
		}
		if page == 20 {
			return nil, errors.New("validation job pagination limit")
		}
	}
	if len(found) != len(checks) {
		return nil, errors.New("required validation jobs missing")
	}
	v := &validation{RunID: m.Ledger.RunID, Attempt: 1, Round: m.Round, Jobs: found}
	if m.Validation != nil && !reflect.DeepEqual(v, m.Validation) {
		return nil, errors.New("validation receipt changed")
	}
	return v, nil
}

func receiptID(m manifest) string {
	m.ID = ""
	b, _ := json.Marshal(m)
	return digest(b)
}

func receiptParents(m manifest) []string {
	var parents []string
	for _, p := range []string{m.Candidate, m.Target, m.SourceHead, m.PreviousSourceHead, m.PreviousState} {
		if p != "" && !slices.Contains(parents, p) {
			parents = append(parents, p)
		}
	}
	return parents
}

func (g gitRepo) seal(ctx context.Context, m *manifest, a github) (string, error) {
	if err := g.verifyCandidate(ctx, *m); err != nil {
		return "", err
	}
	v, err := a.jobs(ctx, *m)
	if err != nil {
		return "", err
	}
	m.Validation = v
	m.ID = receiptID(*m)
	return g.metadata(ctx, *m, receiptParents(*m))
}

func (g gitRepo) verifyReceipt(ctx context.Context, receipt string, m manifest) error {
	v := m.Validation
	if v == nil || m.ID != receiptID(m) || v.RunID <= 0 || v.RunID != m.Ledger.RunID || m.Ledger.Attempt != 1 || v.Attempt != 1 || v.Round != m.Round || len(v.Jobs) != len(checks) {
		return errors.New("incomplete validation receipt")
	}
	seen := map[int64]bool{}
	for _, check := range checks {
		id := v.Jobs[check]
		if id <= 0 || seen[id] {
			return errors.New("invalid validation job identity")
		}
		seen[id] = true
	}
	if err := g.verifyCandidate(ctx, m); err != nil {
		return err
	}
	sealed, err := g.loadManifest(ctx, receipt, true)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(sealed, m) {
		return errors.New("receipt manifest mismatch")
	}
	parents, err := g.git(ctx, "show", "-s", "--format=%P", receipt)
	if err != nil {
		return err
	}
	if parents != strings.Join(receiptParents(m), " ") {
		return errors.New("receipt history retention mismatch")
	}
	return nil
}

func (g gitRepo) currentPolicy(ctx context.Context, remote string, m manifest) error {
	main, err := g.fetch(ctx, remote, "refs/heads/main", "refs/remotes/current-main")
	if err != nil {
		return err
	}
	policy, err := g.policyDigest(ctx, main)
	if err != nil {
		return err
	}
	if policy != m.Policy {
		return errors.New("trusted automation changed; prepare a new candidate")
	}
	return nil
}

func (g gitRepo) reportBody(ctx context.Context, m manifest, status string) (string, error) {
	diff, err := g.git(ctx, "diff", "--stat", m.Target, m.Candidate)
	if err != nil {
		return "", err
	}
	if len(diff) > 16000 {
		diff = diff[:16000] + "\n[truncated; inspect the candidate history]"
	}
	rangediff, err := g.git(ctx, "range-diff", "--no-color", m.SourceBase+".."+m.SourceHead, m.Main+".."+m.Candidate)
	if err != nil {
		return "", err
	}
	if len(rangediff) > 16000 {
		rangediff = rangediff[:16000] + "\n[truncated; inspect the candidate history]"
	}
	report, _ := json.MarshalIndent(struct {
		Adaptations    []adaptation
		BaseGap, Notes []string
	}{m.Adaptations, m.BaseGap, m.Notes}, "", "  ")
	if len(report) > 12000 {
		report = append(report[:12000], []byte("\n[truncated; see signed manifest]")...)
	}
	return fmt.Sprintf("## Native metadata sync: %s\n\n"+
		"Source: %s\nMain: %s\nTarget: %s\nCandidate: %s\n"+
		"Validation: https://github.com/%s/actions/runs/%d (attempt 1, round %d; all five required jobs succeeded).\n"+
		"Claude: %d attempts; $%.4f charged or reserved of $50.\n\n"+
		"Receipt: `%sreceipt/%s`\nRecovery: `%srecovery/%s`\n\n"+
		"Diff:\n%s\n\nSource/candidate range diff (diagnostic, not equivalence proof):\n%s\n\nAdaptation report:\n%s\n",
		status, m.SourceHead, m.Main, m.Target, m.Candidate, repository, m.Ledger.RunID, m.Round, len(m.Ledger.Charges), float64(m.Ledger.total())/1e6,
		botPrefix, m.ID, botPrefix, m.ID, fenced(diff), fenced(rangediff), fenced(string(report))), nil
}

func fenced(s string) string {
	// Never let untrusted commit subjects close the report's code fence.
	fence := "~~~~"
	for strings.Contains(s, fence) {
		fence += "~"
	}
	return fence + "\n" + s + "\n" + fence
}

// Publication receipts recognize historical completion even after later branch updates.
func (g gitRepo) publicationReceipt(ctx context.Context, remote, receipt string, m manifest) (bool, error) {
	values := make([]string, 3)
	for i, ref := range []string{botPrefix + "receipt/" + m.ID, botPrefix + "candidate/" + m.Candidate, botPrefix + "recovery/" + m.ID} {
		value, err := g.remoteRef(ctx, remote, ref)
		if err != nil {
			return false, err
		}
		values[i] = value
	}
	for i, want := range []string{receipt, m.Candidate, m.Target} {
		if values[i] != "" && values[i] != want {
			return false, errors.New("immutable publication ref changed")
		}
	}
	if values[0] == receipt {
		if values[1] != m.Candidate || values[2] != m.Target {
			return false, errors.New("incomplete publication receipt refs")
		}
		return true, nil
	}
	if values[2] != "" {
		return false, errors.New("recovery ref exists without its publication receipt")
	}
	return false, nil
}

func (g gitRepo) publish(ctx context.Context, remote, receipt string, m manifest, validation github) (string, error) {
	if err := g.verifyReceipt(ctx, receipt, m); err != nil {
		return "", err
	}
	applied, err := g.publicationReceipt(ctx, remote, receipt, m)
	if err != nil {
		return "unknown", err
	}
	if applied {
		return "already-published", nil
	}
	for _, ref := range []refUpdate{{m.TargetRef, m.Target, m.Candidate}, {stateRef, m.PreviousState, receipt}} {
		current, e := g.remoteRef(ctx, remote, ref.Ref)
		if e != nil {
			return "unknown", e
		}
		if current != ref.Old {
			return "not-published", errors.New("target or applied state advanced; prepare a new candidate")
		}
	}
	if _, err = validation.jobs(ctx, m); err != nil {
		return "not-published", err
	}
	if err = g.currentPolicy(ctx, remote, m); err != nil {
		return "not-published", err
	}
	if err = ctx.Err(); err != nil {
		return "not-published", err
	}
	pushErr := g.atomicPush(ctx, remote, []refUpdate{
		{m.TargetRef, m.Target, m.Candidate},
		{stateRef, m.PreviousState, receipt},
		{botPrefix + "candidate/" + m.Candidate, "", m.Candidate},
		{botPrefix + "receipt/" + m.ID, "", receipt},
		{botPrefix + "recovery/" + m.ID, "", m.Target},
	})
	// Cancellation or a lost acknowledgement cannot undo an accepted transaction.
	// Read its immutable receipt even when the original context was cancelled.
	checkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	applied, err = g.publicationReceipt(checkCtx, remote, receipt, m)
	if err == nil && applied {
		return "published", nil
	}
	return "unknown", errors.Join(errors.New("publication outcome is unknown; retry only the publish job to reconcile it"), pushErr, err)
}

func (g gitRepo) report(ctx context.Context, out string, m manifest, status string) error {
	body, err := g.reportBody(ctx, m, status)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(out, "report.md"), []byte(body), 0o600); err != nil {
		return err
	}
	if path := os.Getenv("GITHUB_STEP_SUMMARY"); path != "" {
		f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
		if e != nil {
			return e
		}
		_, e = f.WriteString(body)
		return errors.Join(e, f.Close())
	}
	return nil
}

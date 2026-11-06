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
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	repository      = "grafana/mimir-prometheus"
	originURL       = "https://github.com/" + repository + ".git"
	sourceURL       = "https://github.com/aknuds1/prometheus.git"
	upstreamURL     = "https://github.com/prometheus/prometheus.git"
	topic           = "arve/parquet-metadata-resource-attributes"
	botPrefix       = "refs/heads/bot/native-metadata-sync/"
	stateRef        = botPrefix + "state"
	model           = "claude-opus-5-5"
	maxOutputTokens = 32768
	budgetMicroUSD  = 50_000_000
	// Preserve the conservative reservation accepted by existing ledgers.
	reservationMicroUSD = 6_100_000
	maxAttempts         = 40
	schemaVersion       = 2
)

var (
	oidPattern     = regexp.MustCompile(`^[0-9a-f]{40}$`)
	digestPattern  = regexp.MustCompile(`^[0-9a-f]{64}$`)
	checks         = []string{"go", "variants", "lint", "generated", "ui"}
	protectedPaths = []string{
		".github", "tools/native-metadata-sync", "scripts", "Makefile", "Makefile.common",
		".gitattributes", ".gitmodules", ".lfsconfig", ".promu.yml", ".golangci.yml", ".golangci.yaml",
	}
)

//go:embed bootstrap.json
var bootstrapJSON []byte

type projection struct {
	Source  string `json:"source"`
	Local   string `json:"local"`
	Delta   string `json:"delta"`
	Adapted bool   `json:"adapted"`
}

type adaptation struct {
	Source      string `json:"source"`
	Disposition string `json:"disposition"`
	Reason      string `json:"reason"`
}

type charge struct {
	Reserved  int64  `json:"reserved_micro_usd"`
	Actual    *int64 `json:"actual_micro_usd,omitempty"`
	RequestID string `json:"request_id,omitempty"`
}

type ledger struct {
	RunID   int64    `json:"run_id"`
	Attempt int      `json:"attempt"`
	Charges []charge `json:"charges"`
}

type validation struct {
	RunID   int64            `json:"run_id"`
	Attempt int              `json:"attempt"`
	Round   int              `json:"round"`
	Jobs    map[string]int64 `json:"jobs"`
}

// A sealed manifest records validated history; receipt/<id> proves atomic publication.
type manifest struct {
	Version            int          `json:"version"`
	Repository         string       `json:"repository"`
	TargetRef          string       `json:"target_ref"`
	Target             string       `json:"target"`
	PreviousState      string       `json:"previous_state,omitempty"`
	Main               string       `json:"main"`
	Control            string       `json:"control,omitempty"`
	Policy             string       `json:"policy,omitempty"`
	SourceBase         string       `json:"source_base"`
	SourceHead         string       `json:"source_head"`
	PreviousSourceHead string       `json:"previous_source_head,omitempty"`
	OfficialMain       string       `json:"official_main,omitempty"`
	Candidate          string       `json:"candidate"`
	Mapping            []projection `json:"mapping"`
	Overlays           []string     `json:"overlays"`
	Adaptations        []adaptation `json:"adaptations,omitempty"`
	BaseGap            []string     `json:"base_gap,omitempty"`
	Notes              []string     `json:"notes,omitempty"`
	Ledger             ledger       `json:"ledger"`
	Round              int          `json:"round"`
	Created            string       `json:"created"`
	ID                 string       `json:"id,omitempty"`
	Validation         *validation  `json:"validation,omitempty"`
}

func decodeJSON(data []byte, dst any) error {
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(dst); err != nil {
		return err
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("unexpected trailing JSON")
	}
	return nil
}

func writeJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".sync-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func digest(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func (m manifest) valid() error {
	if m.Version != schemaVersion || m.Repository != repository || m.TargetRef != "refs/heads/"+topic {
		return errors.New("unexpected manifest version, repository, or target")
	}
	for _, s := range []string{m.Target, m.Main, m.SourceBase, m.SourceHead, m.Candidate} {
		if !oidPattern.MatchString(s) {
			return fmt.Errorf("invalid commit ID %q", s)
		}
	}
	for _, s := range []string{m.PreviousState, m.Control, m.OfficialMain, m.PreviousSourceHead} {
		if s != "" && !oidPattern.MatchString(s) {
			return errors.New("invalid optional commit ID")
		}
	}
	if m.Policy != "" && !digestPattern.MatchString(m.Policy) {
		return errors.New("invalid policy digest")
	}
	if m.ID != "" && !digestPattern.MatchString(m.ID) {
		return errors.New("invalid receipt identity")
	}
	if m.Round < 0 || m.Round > 2 {
		return errors.New("invalid repair round")
	}
	seen := map[string]bool{}
	for _, p := range m.Mapping {
		if !oidPattern.MatchString(p.Source) || !oidPattern.MatchString(p.Local) || !digestPattern.MatchString(p.Delta) || seen[p.Source] {
			return errors.New("invalid or duplicate projection")
		}
		seen[p.Source] = true
	}
	for _, s := range m.Overlays {
		if !oidPattern.MatchString(s) {
			return errors.New("invalid overlay")
		}
	}
	return m.Ledger.valid()
}

func (l ledger) total() int64 {
	var total int64
	for _, c := range l.Charges {
		if c.Actual == nil {
			total += c.Reserved
		} else {
			total += *c.Actual
		}
	}
	return total
}

func (l ledger) valid() error {
	if len(l.Charges) > maxAttempts {
		return errors.New("API attempt limit exceeded")
	}
	for _, c := range l.Charges {
		if c.Reserved != reservationMicroUSD || c.Actual != nil && (*c.Actual < 0 || *c.Actual > c.Reserved) {
			return errors.New("invalid API charge")
		}
	}
	if l.total() > budgetMicroUSD {
		return errors.New("API budget exceeded")
	}
	return nil
}

func (l *ledger) reserve() (int, error) {
	if err := l.valid(); err != nil {
		return 0, err
	}
	if l.RunID <= 0 || l.Attempt != 1 {
		return 0, errors.New("paid calls require an original workflow attempt and a run ledger")
	}
	if len(l.Charges) == maxAttempts || l.total()+reservationMicroUSD > budgetMicroUSD {
		return 0, errors.New("insufficient API budget or attempts; no request sent")
	}
	l.Charges = append(l.Charges, charge{Reserved: reservationMicroUSD})
	return len(l.Charges) - 1, nil
}

func protected(path string) bool {
	for _, p := range protectedPaths {
		if path == p || strings.HasPrefix(path, p+"/") {
			return true
		}
	}
	return false
}

func bootstrap() (manifest, error) {
	var m manifest
	err := decodeJSON(bootstrapJSON, &m)
	if err == nil {
		err = m.valid()
	}
	return m, err
}

package validation

import (
	"fmt"
	"unicode/utf8"

	"github.com/prometheus/common/model"
)

// NamingScheme that is used for validation of metric and label names.
type NamingScheme string

const (
	// LegacyNamingScheme validates metric and label names according to the legacy naming convention.
	LegacyNamingScheme NamingScheme = "legacy"
	// UTF8NamingScheme validates metric and label names according to UTF8 naming convention.
	UTF8NamingScheme NamingScheme = "utf8"
)

// Validate the NamingScheme is one of LegacyNamingScheme, UTF8NamingScheme, or unset ("").
// If s is unset, NamingScheme defaults to UTF8NamingScheme.
func (s NamingScheme) Validate() error {
	switch s {
	case "", LegacyNamingScheme, UTF8NamingScheme:
		return nil
	}
	return fmt.Errorf("invalid name validation scheme %q", s)
}

// WithDefault returns s if it is set (s != ""), defaultScheme otherwise.
func (s NamingScheme) WithDefault(defaultScheme NamingScheme) NamingScheme {
	if s == "" {
		return defaultScheme
	}
	return s
}

// IsValidLabelName ensures name adheres to the NamingScheme.
func (s NamingScheme) IsValidLabelName(name string) bool {
	if s == LegacyNamingScheme {
		return model.LabelName(name).IsValidLegacy()
	}
	return len(name) > 0 && utf8.ValidString(name)
}

// IsValidMetricName ensures name adheres to the NamingScheme.
func (s NamingScheme) IsValidMetricName(name string) bool {
	if s == LegacyNamingScheme {
		return model.IsValidLegacyMetricName(name)
	}
	return len(name) > 0 && utf8.ValidString(name)
}

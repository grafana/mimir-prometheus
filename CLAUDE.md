# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
make build                    # Full build with embedded assets
go build ./cmd/prometheus/    # Quick build without assets (for development)
go build ./cmd/promtool/      # Build utility tool
```

## Testing

```bash
make test                     # Full test suite with race detector and UI tests
make test-short               # Short tests only
go test ./tsdb/...            # Test specific package
go test -run TestName ./pkg/  # Run single test
go test -race ./...           # With race detector
go test -v ./pkg/             # Verbose output
go test --tags=dedupelabels ./...  # With build tags
```

- Do NOT use `-count=1` unless there is a specific reason to bypass the test cache (e.g., debugging flaky tests). Go's test caching is beneficial and should be left enabled by default.

## Linting and Formatting

```bash
make lint                     # Run golangci-lint
make format                   # Run gofmt and lint fixes
make lint-fix                 # Auto-fix linting issues
```

## Parser Generation

After modifying `promql/parser/generated_parser.y`:
```bash
make parser                   # Regenerate parser via goyacc
make check-generated-parser   # Verify parser is up-to-date
```

## Architecture Overview

Prometheus is a monitoring system with these core components:

### Data Flow
```
Service Discovery → Scrape → Storage (TSDB) → PromQL Engine → Web API/UI
                                    ↓
                              Rules Engine → Notifier → Alertmanager
```

### Key Packages

- **cmd/prometheus/**: Main server entry point
- **cmd/promtool/**: CLI utility for validation, TSDB operations
- **storage/**: Storage abstraction layer with `Appender`, `Querier` interfaces
- **tsdb/**: Time series database (head block, WAL, compaction, chunks)
- **tsdb/seriesmetadata/**: Series metadata, OTel resource/scope attribute, and entity persistence
- **promql/**: Query engine and PromQL parser
- **scrape/**: Metric collection from targets
- **discovery/**: 30+ service discovery implementations
- **rules/**: Alerting and recording rule evaluation
- **notifier/**: Alert routing to Alertmanager
- **config/**: YAML configuration parsing
- **model/labels/**: Label storage and manipulation
- **model/histogram/**: Native histogram types
- **web/**: HTTP API and React/Mantine UI
- **storage/remote/otlptranslator/**: OTLP to Prometheus translation including CombinedAppender

### Storage Interface Evolution

The storage layer is transitioning from `Appender` (V1) to `AppenderV2`. New code should use `AppenderV2` which combines sample, histogram, and exemplar appending into a single method. The `ResourceQuerier` interface (in `storage/interface.go`) provides `GetResourceAt()` and `IterUniqueAttributeNames()` for querying stored resource data. `ResourceUpdater` provides `UpdateResource()` for ingesting resource attributes with entities.

### TSDB Structure

- **Head Block**: In-memory storage for recent data with WAL
- **Persistent Blocks**: Immutable on-disk blocks
- **Compaction**: Merges blocks and applies retention
- **Chunks**: Gorilla-compressed time series data
- **Series Metadata**: Optional Parquet-based storage for metric metadata and OTel resource attributes

### OTel Native Metadata

Prometheus supports persisting OTel resource attributes, instrumentation scopes, and entities per time series. Enabled via `--enable-feature=native-metadata`.

- **Feature Flag**: `--enable-feature=native-metadata` sets `EnableNativeMetadata` on TSDB and web config
- **Storage**: Parquet-based sidecar files in `tsdb/seriesmetadata/` alongside TSDB blocks
- **Resources**: `UpdateResource()` on `storage.ResourceUpdater` ingests identifying/descriptive attributes plus entities. Data is versioned over time per series (`VersionedResource` → `[]ResourceVersion`); `AddOrExtend()` creates a new version when attributes change or extends the time range when they match
- **Scopes**: `UpdateScope()` ingests OTel InstrumentationScope data (name, version, schema URL, attributes). Stored as `VersionedScope` → `[]ScopeVersion` in `MemScopeStore`
- **Entities**: `Entity` type in `tsdb/seriesmetadata/entity.go` with 7 predefined types: `resource`, `service`, `host`, `container`, `k8s.pod`, `k8s.node`, `process`. Each entity has typed ID (identifying) and Description (descriptive) attribute maps. Entities are embedded in `ResourceVersion`
- **Identifying Attributes**: `service.name`, `service.namespace`, `service.instance.id` used for resource identification
- **info() Function**: PromQL experimental function to enrich metrics with resource/scope attributes. Three modes controlled by `--query.info-resource-strategy`:
  - `target-info` (default): metric-join against `target_info` only (no native metadata needed)
  - `resource-attributes`: uses only stored native metadata via `ResourceQuerier`
  - `hybrid`: combines native metadata for `target_info` with metric-join for other info metrics; native metadata takes precedence on conflicts
  - Mode is selected per-call by `classifyInfoMode()` in `promql/info.go` based on `__name__` matchers
- **Label Name Translation**: `LabelNamerConfig` in `promql/engine.go` controls mapping OTel attribute names to Prometheus label names (UTF-8 handling, underscore sanitization). Used by `buildAttrNameMappings()` to create bidirectional name mappings
- **API Endpoint**: `/api/v1/resources` for querying stored attributes (supports `format=attributes` for autocomplete). Returns 400 when native metadata is disabled
- **OTLP Integration**: `CombinedAppender` in `storage/remote/otlptranslator/prometheusremotewrite/` handles OTLP ingestion
- **Observability**: Instrumentation metrics for monitoring the metadata pipeline:
  - `prometheus_tsdb_head_resource_updates_committed_total` — resource attribute updates committed
  - `prometheus_tsdb_head_scope_updates_committed_total` — scope updates committed
  - `prometheus_tsdb_storage_series_metadata_bytes` — bytes used by Parquet metadata files across all blocks
  - `prometheus_engine_info_function_calls_total{mode}` — info() calls by resolution mode (`native`, `metric-join`, `hybrid`)

Demo examples in `documentation/examples/`:
- `info-autocomplete-demo/`: Interactive demo for info() function autocomplete
- `otlp-resource-attributes/`: OTLP ingestion with resource attributes
- `metadata-persistence/`: Basic metadata persistence demo

### Parquet Usage: parquet-common vs tsdb/seriesmetadata

These two systems both use `parquet-go` but solve different problems and should not be merged:

- **parquet-common** (`github.com/prometheus-community/parquet-common`): Replaces entire TSDB block format (labels + sample chunks) with columnar Parquet for cloud-scale analytical storage (Cortex/Thanos). Dynamic schema with one column per label name. Uses advanced Parquet features (projections, row group stats, bloom filters, page-level I/O).
- **tsdb/seriesmetadata**: Small sidecar Parquet file alongside standard TSDB blocks storing metric metadata and OTel resource attributes. Static struct-based schema with nested lists. Loads entire file into memory for O(1) hash lookup. Typically kilobytes, not gigabytes.

**Why they can't converge**: Incompatible schemas (columnar per-label vs row-oriented with nested lists), incompatible scale assumptions (distributed cloud vs single-node local), and resource attributes are versioned (multiple values over time per series) which doesn't fit parquet-common's one-value-per-row label model. parquet-common exposes no reusable Parquet I/O primitives — its API is purpose-built for time series data.

**Techniques ported from parquet-common to seriesmetadata**:
- Explicit zstd compression (`zstd.SpeedBetterCompression`) instead of parquet-go defaults
- Row sorting before write (by namespace, labels_hash, content_hash, MinTime) for better compression
- Footer key-value metadata for schema evolution and row counts
- Namespace-partitioned row groups: each namespace written as separate row group(s) via `WriteFileWithOptions`, enabling selective reads
- Optional bloom filters on `labels_hash` and `content_hash` columns (`WriterOptions.EnableBloomFilters`). Write-only in this package; querying is done by the consumer (e.g., Mimir store-gateway)
- Configurable row group size limits (`WriterOptions.MaxRowsPerRowGroup`) for bounded memory on read
- `io.ReaderAt` read API (`ReadSeriesMetadataFromReaderAt`) decouples from `*os.File`, enabling `objstore.Bucket`-backed readers
- Namespace filtering on read (`WithNamespaceFilter`) skips non-matching row groups using Parquet column index min/max bounds

**Not ported** (inapplicable to this schema): Column projections (fixed ~15 column schema, not 500+ dynamic columns), two-file projections, page-level I/O (row groups are KB-to-MB, not GB), sharding.

**Normalized Parquet Storage**: The Parquet file uses content-addressed tables to eliminate cross-series duplication of resources and scopes. Five namespace types: `metric` (unchanged), `resource_table` (unique resource content keyed by xxhash `ContentHash`), `resource_mapping` (series `LabelsHash` → `ContentHash` + time range), `scope_table`, `scope_mapping`. N series sharing the same OTel resource produce 1 table row + N mapping rows instead of N full copies. The in-memory model remains denormalized (`MemResourceStore`, `MemScopeStore` unchanged) — normalization happens only during `WriteFile()` and is reversed during read. Content hashing uses `xxhash.Sum64` with sorted keys for determinism. Footer metadata tracks `resource_table_count`, `resource_mapping_count`, `scope_table_count`, `scope_mapping_count`.

**Distributed-Scale Considerations**: The namespace-partitioned row groups, bloom filters, `io.ReaderAt` API, and namespace filtering are designed for object-storage access patterns in clustered HA implementations (e.g., Grafana Mimir store-gateway). A Mimir integration would wrap `objstore.Bucket` as `io.ReaderAt`, use `WithNamespaceFilter` to read only the needed namespaces, and query bloom filters at the store-gateway layer to skip row groups before deserialization.

### Build Tags

- `dedupelabels`, `slicelabels`: Label storage variants
- `forcedirectio`: Direct I/O for TSDB
- `builtinassets`: Embed web assets in binary

## Test Patterns

- Tests use `testify/require` for assertions
- Test helpers in `util/testutil/` and `util/teststorage/`
- Head tests use `newTestHead()` which auto-cleans up via `t.Cleanup`
- Table-driven tests are common throughout

## Pull Requests and Issues

- Use the PR template in `.github/PULL_REQUEST_TEMPLATE.md`
- Use the issue templates in `.github/ISSUE_TEMPLATE/` (bug reports, feature requests)
- PR titles should follow the format `area: short description` (e.g., `tsdb: reduce disk usage`)
- Sign commits with `-s` / `--signoff` for DCO compliance
- Do not mention Claude or AI assistance in commits, issues, or PRs

## Fork: grafana/mimir-prometheus

This repository is `grafana/mimir-prometheus`, a Grafana-maintained fork of `prometheus/prometheus`. It is vendored into [grafana/mimir](https://github.com/grafana/mimir) as a dependency.

- **Module path**: Remains `github.com/prometheus/prometheus` (unchanged from upstream)
- **Upstream sync**: A weekly automated workflow (`merge-upstream-prometheus.yml`) merges `prometheus/prometheus` main into this fork's main. Merge conflicts produce a draft PR with resolution instructions
- **Mimir integration**: Pushes to `main` or `r*` branches trigger `trigger-mimir-update.yml`, which sends a `repository_dispatch` to `grafana/mimir` to update the vendored copy
- **Where to PR**: Changes specific to Mimir go to `grafana/mimir-prometheus`. Changes intended for upstream Prometheus should be PRed to `prometheus/prometheus` directly

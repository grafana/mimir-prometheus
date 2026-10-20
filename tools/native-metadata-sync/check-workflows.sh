#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
# actionlint 1.7.12 predates these supported GitHub syntax additions:
# https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#cache-mode
# https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency
# Keep the exceptions limited to the two unknown-key diagnostics.
actionlint \
  -ignore '^unexpected key "cache-mode" for "workflow" section\.' \
  -ignore '^unexpected key "queue" for "concurrency" section\.' \
  .github/workflows/sync-native-metadata.yml \
  .github/workflows/validate-native-metadata-sync.yml \
  .github/workflows/native-metadata-sync-ci.yml

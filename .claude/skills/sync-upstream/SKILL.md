---
name: sync-upstream
description: Sync the local feature branch from aknuds1/arve/parquet-metadata-resource-attributes by cherry-picking new upstream commits
disable-model-invocation: true
argument-hint: [--dry-run]
allowed-tools: Bash(git fetch:*), Bash(git log:*), Bash(git checkout:*), Bash(git cherry-pick:*), Bash(GIT_EDITOR=true git cherry-pick:*), Bash(git push:*), Bash(git merge-base:*), Bash(git diff:*), Bash(git branch:*), Bash(git rev-parse:*), Bash(git reset:*), Bash(diff:*), Bash(go build:*)
---

# Sync local feature branch from aknuds1 remote

## Overview

This procedure syncs new feature commits from the upstream
`aknuds1/arve/parquet-metadata-resource-attributes` branch (based on `prometheus/main`,
i.e. aknuds1/prometheus) to the local `arve/parquet-metadata-resource-attributes`
branch (based on `origin/main`, i.e. grafana/mimir-prometheus).

The `aknuds1` branch is the source of truth (upstream PR target). The local branch
is a cherry-pick of those same feature commits onto `origin/main`. When new commits
appear on `aknuds1`, we cherry-pick them onto our local branch.

## Arguments

- `--dry-run`: Show what would be done without making changes (default if no argument)

## Procedure

### 1. Fetch latest state

```bash
git fetch origin
git fetch aknuds1
git fetch prometheus
```

### 2. Identify upstream feature commits

The upstream feature commits sit between `prometheus/main` and `aknuds1/arve/parquet-metadata-resource-attributes`:

```bash
git log --oneline --reverse prometheus/main..aknuds1/arve/parquet-metadata-resource-attributes
```

### 3. Identify local feature commits

The local feature commits sit between the merge-base with `origin/main` and `HEAD`:

```bash
MERGE_BASE=$(git merge-base origin/main arve/parquet-metadata-resource-attributes)
git log --oneline --reverse ${MERGE_BASE}..arve/parquet-metadata-resource-attributes
```

### 4. Compare by subject to find drift

```bash
diff <(git log --format='%s' --reverse prometheus/main..aknuds1/arve/parquet-metadata-resource-attributes) \
     <(git log --format='%s' --reverse ${MERGE_BASE}..arve/parquet-metadata-resource-attributes)
```

- Lines only in the left (aknuds1): new upstream commits to cherry-pick
- Lines only in the right (local): local-only commits (e.g. skill/docs commits)
- If all upstream subjects appear in local, branches are in sync

### 5. Cherry-pick missing upstream commits (skip if --dry-run)

If aknuds1 has new commits not present locally, cherry-pick them onto the local branch.
Identify the specific commit SHAs from the aknuds1 branch whose subjects are missing
locally, then:

```bash
git checkout arve/parquet-metadata-resource-attributes
git cherry-pick <sha1> <sha2> ...
```

If conflicts arise, resolve them. CLAUDE.md conflicts are expected since many commits
modify it. Use `git checkout --ours CLAUDE.md && git add CLAUDE.md` to keep the
local state — later commits will overwrite with the latest content.
Do NOT use `--theirs`, which would replace the file with a snapshot from the
aknuds1-based branch, potentially losing local-specific content.

### 6. Reorder local-only commits to the top (skip if --dry-run)

Local-only commits (skills, docs, etc.) must always sit on top of the upstream
cherry-picks. After cherry-picking, if any local-only commits are interleaved with
or below the new upstream commits, reorder them to the top:

```bash
# Count local-only commits that need to move
# Reset back past both the new cherry-picks and the local-only commits
git reset --hard HEAD~<N>  # N = new cherry-picks + local-only commits
# Re-apply: upstream cherry-picks first, then local-only commits
git cherry-pick <upstream-sha1> <upstream-sha2> ... <local-sha1> <local-sha2> ...
```

This keeps the history clean: all upstream feature commits in order, followed by
all local-only commits.

### 7. Report local-only commits

List any commits present locally but not on aknuds1 (by subject). These are expected
for local-only changes like skill definitions or docs commits. Report them to the
user but take no action.

### 8. Verify

```bash
# Code compiles
go build ./tsdb/

# All upstream subjects now present locally
diff <(git log --format='%s' --reverse prometheus/main..aknuds1/arve/parquet-metadata-resource-attributes) \
     <(git log --format='%s' --reverse ${MERGE_BASE}..HEAD | head -n $(git log --oneline prometheus/main..aknuds1/arve/parquet-metadata-resource-attributes | wc -l | tr -d ' '))
```

The second check compares upstream subjects against the first N local subjects
(where N = upstream count), ignoring local-only trailing commits.

## Important notes

- The aknuds1 branch is the source of truth; we never push to it
- Cherry-pick direction is always aknuds1 → local
- Local-only commits (skills, docs) are expected and should not be synced upstream
- The local branch will have different commit SHAs than aknuds1 (different parent chain)
- CLAUDE.md is the main source of conflicts since it's updated in many commits

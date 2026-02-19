#!/usr/bin/env bash
# update-claude-memory.sh
# Regenerates .claude/memory/recent-changes.md from git state.
# Usage:
#   Manual:  bash scripts/update-claude-memory.sh
#   Auto:    triggered by Claude Code PostToolUse hook after every Edit/Write

set -euo pipefail

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
MEMORY_DIR="$REPO_ROOT/.claude/memory"
OUTPUT="$MEMORY_DIR/recent-changes.md"

mkdir -p "$MEMORY_DIR"

{
  echo "# Recent Changes"
  echo ""
  echo "> Auto-generated — do not edit manually. Run \`scripts/update-claude-memory.sh\` to refresh."
  echo "> Last updated: $(date '+%Y-%m-%d %H:%M:%S')"
  echo ""

  echo "## Recent Git Commits (newest first)"
  echo ""
  echo '```'
  git -C "$REPO_ROOT" log --oneline -20 2>/dev/null || echo "(no commits yet)"
  echo '```'
  echo ""

  echo "## Uncommitted Workspace Changes"
  echo ""
  STAGED=$(git -C "$REPO_ROOT" diff --cached --name-only 2>/dev/null)
  UNSTAGED=$(git -C "$REPO_ROOT" diff --name-only 2>/dev/null)
  UNTRACKED=$(git -C "$REPO_ROOT" ls-files --others --exclude-standard 2>/dev/null)

  if [ -z "$STAGED" ] && [ -z "$UNSTAGED" ] && [ -z "$UNTRACKED" ]; then
    echo "_Working tree is clean — no uncommitted changes._"
  else
    if [ -n "$STAGED" ]; then
      echo "### Staged (ready to commit)"
      echo '```'
      echo "$STAGED"
      echo '```'
    fi
    if [ -n "$UNSTAGED" ]; then
      echo "### Modified (not staged)"
      echo '```'
      echo "$UNSTAGED"
      echo '```'
    fi
    if [ -n "$UNTRACKED" ]; then
      echo "### New files (untracked)"
      echo '```'
      echo "$UNTRACKED"
      echo '```'
    fi
  fi
  echo ""

  echo "## Source Files Changed in the Last 7 Days"
  echo ""
  echo '```'
  git -C "$REPO_ROOT" log \
    --since="7 days ago" \
    --name-only \
    --pretty=format: \
    -- '*.py' '*.rs' '*.ts' '*.tsx' \
    2>/dev/null \
    | sort -u \
    | grep -v '^$' \
    | head -40 \
    || echo "(none)"
  echo '```'

} > "$OUTPUT"

echo "Updated: $OUTPUT"

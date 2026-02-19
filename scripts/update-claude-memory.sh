#!/usr/bin/env bash
# update-claude-memory.sh
# 更新 .claude/memory/recent-changes.md
# 用法：
#   手动运行：bash scripts/update-claude-memory.sh
#   由 Claude Code PostToolUse hook 自动调用

set -euo pipefail

REPO_ROOT="$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
MEMORY_DIR="$REPO_ROOT/.claude/memory"
OUTPUT="$MEMORY_DIR/recent-changes.md"

mkdir -p "$MEMORY_DIR"

{
  echo "# 最近变更记录"
  echo ""
  echo "> 自动生成，每次代码变更后更新。运行 \`scripts/update-claude-memory.sh\` 手动刷新。"
  echo "> 最后更新：$(date '+%Y-%m-%d %H:%M:%S')"
  echo ""

  echo "## 最近 Git 提交（最新在前）"
  echo ""
  echo '```'
  git -C "$REPO_ROOT" log --oneline -20 2>/dev/null || echo "(no commits yet)"
  echo '```'
  echo ""

  echo "## 工作区未提交变更"
  echo ""
  STAGED=$(git -C "$REPO_ROOT" diff --cached --name-only 2>/dev/null)
  UNSTAGED=$(git -C "$REPO_ROOT" diff --name-only 2>/dev/null)
  UNTRACKED=$(git -C "$REPO_ROOT" ls-files --others --exclude-standard 2>/dev/null)

  if [ -z "$STAGED" ] && [ -z "$UNSTAGED" ] && [ -z "$UNTRACKED" ]; then
    echo "_工作区干净，无未提交变更。_"
  else
    if [ -n "$STAGED" ]; then
      echo "### 已暂存（待提交）"
      echo '```'
      echo "$STAGED"
      echo '```'
    fi
    if [ -n "$UNSTAGED" ]; then
      echo "### 已修改（未暂存）"
      echo '```'
      echo "$UNSTAGED"
      echo '```'
    fi
    if [ -n "$UNTRACKED" ]; then
      echo "### 新文件（未跟踪）"
      echo '```'
      echo "$UNTRACKED"
      echo '```'
    fi
  fi
  echo ""

  echo "## 最近 7 天修改的源文件"
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
    || echo "(无)"
  echo '```'

} > "$OUTPUT"

echo "已更新：$OUTPUT"

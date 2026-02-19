# Claude Code Instructions

Read `AGENTS.md` for full project context (architecture, patterns, conventions).

## 模块索引（快速查找文件和类）

> `.claude/memory/` 目录包含层次化索引，供 Claude Code 快速定位代码。每次代码变更后自动刷新。

| 索引文件 | 内容 |
|----------|------|
| `.claude/memory/engine-index.md` | Engine 所有模块：公开 API、core、operators、runtime、serve、webui、queue |
| `.claude/memory/control-index.md` | Control 平面：路由、模型、Schema、服务层 |
| `.claude/memory/lib-index.md` | WorkQueue（Rust O(1) 约束）、RayDP |
| `.claude/memory/recent-changes.md` | 最近 git 提交 + 工作区变更（自动更新） |

**手动刷新**：`bash scripts/update-claude-memory.sh`

---

## Quick Reference

- **Monorepo**: `engine/` (Ray processing), `control/` (FastAPI), `lib/` (workqueue-rs, raydp)
- **Engine source**: `engine/_internal/` (NOT `engine/engine/`)
- **Public API**: `engine/nurion/__init__.py`
- **Package manager**: uv
- **Linting**: `cd engine && uv run ruff check _internal/`
- **Tests**: `cd engine && uv run pytest tests/ -v --tb=short -m "not integration"`

## Rules

- Communicate in Chinese unless user switches to English
- Commit messages: Conventional Commits (`feat:`, `fix:`, `refactor:`, etc.)
- Don't over-engineer; only implement what's requested
- Operators are config-driven and stateless (`OperatorConfig` + `OperatorRuntime`)
- WorkQueue hot paths must be O(1) — never scan
- Check `engine/design-docs/` before proposing architectural changes

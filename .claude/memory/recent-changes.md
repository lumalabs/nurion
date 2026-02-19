# 最近变更记录

> 自动生成，每次代码变更后更新。运行 `scripts/update-claude-memory.sh` 手动刷新。
> 最后更新：2026-02-19 08:02:48

## 最近 Git 提交（最新在前）

```
4d114ac chore: upgrade pyiceberg to 0.11.0 (#54)
3a10aed refactor: new webui (#53)
c247d0f chore: refactor webui API (#52)
b105838 test: add integration tests for inference (#51)
d5902c9 feat: support smart gpu scheduling (#50)
ec804d1 chore: optimize pool logic (#49)
b51fce9 chore: enhance pool scale up/down logic (#48)
b42b111 chore: detached mode for serve (#46)
049b234 feat: support s3/fs as payloadstore (#45)
eedce94 refactor: rename subfolder names (#44)
db71d8e feat: use serve union find for minhash dedup (#43)
863c66f chore: open mypy check (#42)
3e14340 refactor: remove checkpoint management code and update agent guidelines (#41)
79f8838 chore: fix some ugly design (#40)
c4eff88 feat: support serve vllm (#39)
bf78e5f feat: redesign backpressure & webui (#38)
c36be4a refactor: remove outdated code (#37)
b9efccb chore: optimize claim API (#36)
893f448 feat: use new queue implement replace tansu (#35)
f7de8ef fix: partition assign error in multi workers (#34)
```

## 工作区未提交变更

### 已修改（未暂存）
```
.claude/settings.json
CLAUDE.md
```
### 新文件（未跟踪）
```
.claude/memory/control-index.md
.claude/memory/engine-index.md
.claude/memory/lib-index.md
.claude/memory/recent-changes.md
scripts/update-claude-memory.sh
```

## 最近 7 天修改的源文件

```
.agents/skills/webapp-testing/examples/console_logging.py
.agents/skills/webapp-testing/examples/element_discovery.py
.agents/skills/webapp-testing/examples/static_html_automation.py
.agents/skills/webapp-testing/scripts/with_server.py
engine/_internal/core/stage_master.py
engine/_internal/core/stage_worker.py
engine/_internal/operators/llm/client.py
engine/_internal/operators/llm/operator.py
engine/_internal/runtime/backpressure.py
engine/_internal/runtime/ray_runner.py
engine/_internal/serve/__init__.py
engine/_internal/serve/allocator.py
engine/_internal/serve/config.py
engine/_internal/serve/fake_server.py
engine/_internal/serve/manager.py
engine/_internal/serve/pool.py
engine/_internal/serve/worker.py
engine/_internal/webui/api/events.py
engine/_internal/webui/api/exceptions.py
engine/_internal/webui/api/lineage.py
engine/_internal/webui/api/serve.py
engine/_internal/webui/api/stages.py
engine/_internal/webui/api/workers.py
engine/_internal/webui/app.py
engine/_internal/webui/frontend/src/App.tsx
engine/_internal/webui/frontend/src/api/client.ts
engine/_internal/webui/frontend/src/api/events.ts
engine/_internal/webui/frontend/src/api/jobs.ts
engine/_internal/webui/frontend/src/api/lineage.ts
engine/_internal/webui/frontend/src/api/serve.ts
engine/_internal/webui/frontend/src/api/stages.ts
engine/_internal/webui/frontend/src/api/types.ts
engine/_internal/webui/frontend/src/api/workers.ts
engine/_internal/webui/frontend/src/components/DAGVisualization.tsx
engine/_internal/webui/frontend/src/components/LogViewer.tsx
engine/_internal/webui/frontend/src/components/MetricCards.tsx
engine/_internal/webui/frontend/src/components/QueueStatsBar.tsx
engine/_internal/webui/frontend/src/components/StatusBadge.tsx
engine/_internal/webui/frontend/src/layouts/AppLayout.tsx
engine/_internal/webui/frontend/src/main.tsx
```

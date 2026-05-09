# DriftBench 服务台

本目录提供一个轻量级本地 Web 服务，用来通过浏览器调用 DriftBench。

## 启动

```bash
python driftbench_service/server.py --port 8000
```

浏览器打开 `http://127.0.0.1:8000`。

## 说明

- 支持 `run-yaml` 与 `trace-to-spec`。
- 支持从 CSV/Postgres 抽取 schema（分布统计）。
- 支持 CSV / DB 配置文件上传或手动粘贴保存到 `driftbench_service/uploads/`。
- Schema 默认输出到 `driftbench_service/schemas/`。
- 路径必须位于仓库内部，支持相对路径。
- 日志默认保留最近 2000 行。
- 任务列表会持久化到 `driftbench_service/state/jobs.json`，服务重启后仍可查看历史任务。
- 服务重启前处于 `queued/running` 的任务会标记为 `interrupted`（不可自动恢复）。
- 新增公开 Spec 目录接口：
  - `GET /api/public-specs`（支持 `tag/query/limit` 过滤）
  - `POST /api/public-specs/import-run`（按 `spec_id` 或 `spec_path` 导入，支持可选执行）

## 可选环境变量

- `PYTHON`: 指定运行 DriftBench 的 Python 可执行路径。
- `DRIFTBENCH_PORT`: 默认端口。

## 注意

- Postgres schema 抽取依赖 `psycopg2`。

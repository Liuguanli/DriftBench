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
- 任务列表仅保存在内存中，重启服务会清空。

## 可选环境变量

- `PYTHON`: 指定运行 DriftBench 的 Python 可执行路径。
- `DRIFTBENCH_PORT`: 默认端口。

## 注意

- Postgres schema 抽取依赖 `psycopg2`。

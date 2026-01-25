#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import os
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing env var: {name}")
    return value


def _log_line(handle, message: str) -> None:
    ts = dt.datetime.now().isoformat()
    handle.write(f"[{ts}] {message}\n")
    handle.flush()


def _iter_rows(path: Path) -> Tuple[List[str], Iterable[dict]]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"Missing header in {path}")
        rows = list(reader)
    return reader.fieldnames, rows


def _sort_rows(rows: List[dict], ts_key: str) -> List[dict]:
    if not rows:
        return rows
    if ts_key not in rows[0]:
        return rows
    return sorted(rows, key=lambda r: int(r.get(ts_key) or 0))


def _connect_pg(database_url: str):
    try:
        import psycopg2  # type: ignore
    except Exception as exc:  # pragma: no cover
        raise SystemExit(f"psycopg2 is required to apply SQL: {exc}")
    return psycopg2.connect(database_url)


def _apply_file(
    cur,
    conn,
    path: Path,
    log_dir: Path,
    commit_every: int,
    log_sql: bool,
    progress_every: int,
) -> int:
    headers, rows = _iter_rows(path)
    rows = _sort_rows(rows, "ts_ns")
    if "sql" not in headers:
        raise SystemExit(f"Missing 'sql' column in {path}")

    file_log = log_dir / f"apply_{path.stem}.log"
    sql_log_handle = None
    sql_writer = None
    if log_sql:
        sql_log_path = log_dir / f"apply_{path.stem}.sql.csv"
        sql_log_handle = sql_log_path.open("w", encoding="utf-8", newline="")
        sql_writer = csv.writer(sql_log_handle)
        sql_writer.writerow(["idx", "ts_ns", "sql"])

    with file_log.open("w", encoding="utf-8", newline="") as flog:
        _log_line(flog, f"Start {path}")
        applied = 0
        for idx, row in enumerate(rows):
            sql = (row.get("sql") or "").strip()
            if not sql:
                continue
            cur.execute(sql)
            if log_sql and sql_writer:
                sql_writer.writerow([idx, row.get("ts_ns", ""), sql])
                sql_log_handle.flush()
            applied += 1
            if applied % commit_every == 0:
                conn.commit()
                _log_line(flog, f"Commit at {applied} statements")
            if progress_every and applied % progress_every == 0:
                _log_line(flog, f"Progress {applied} statements")
        conn.commit()
        _log_line(flog, f"Done {path} (statements={applied})")
    if sql_log_handle:
        sql_log_handle.close()
    return applied


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply SQL update CSVs in order.")
    parser.add_argument(
        "--plan-dir",
        default=os.environ.get("SQL_PLAN_DIR", ""),
    )
    parser.add_argument(
        "--pattern",
        default="*_updates.csv",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL", ""),
    )
    parser.add_argument(
        "--log-dir",
        default="",
    )
    parser.add_argument("--commit-every", type=int, default=1000)
    parser.add_argument("--progress-every", type=int, default=5000)
    parser.add_argument(
        "--log-sql",
        action="store_true",
        default=os.environ.get("LOG_SQL", "0") == "1",
    )
    args = parser.parse_args()

    plan_dir = Path(args.plan_dir or "")
    if not plan_dir.exists():
        raise SystemExit(f"SQL plan dir not found: {plan_dir}")

    log_dir = Path(args.log_dir or "")
    if not log_dir:
        output_root = _require_env("OUTPUT_ROOT")
        log_dir = Path(output_root) / "log" / "ce_timeline"
    log_dir.mkdir(parents=True, exist_ok=True)

    database_url = args.database_url or _require_env("DATABASE_URL")
    files = sorted(plan_dir.glob(args.pattern))
    if not files:
        raise SystemExit(f"No SQL plan files matching {args.pattern} in {plan_dir}")

    run_log = log_dir / "apply_updates.log"
    with run_log.open("w", encoding="utf-8") as log:
        _log_line(log, f"Plan dir: {plan_dir}")
        _log_line(log, f"Pattern: {args.pattern}")
        _log_line(log, f"Files: {len(files)}")
        _log_line(log, f"Commit every: {args.commit_every}")
        _log_line(log, f"Log SQL: {args.log_sql}")

        conn = _connect_pg(database_url)
        conn.autocommit = False
        cur = conn.cursor()

        total = 0
        for path in files:
            _log_line(log, f"Apply {path.name}")
            applied = _apply_file(
                cur,
                conn,
                path,
                log_dir,
                args.commit_every,
                args.log_sql,
                args.progress_every,
            )
            total += applied
            _log_line(log, f"Applied {applied} statements from {path.name}")

        cur.close()
        conn.close()
        _log_line(log, f"All done. Total statements: {total}")


if __name__ == "__main__":
    main()

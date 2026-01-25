#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import os
import re
import shutil
import sys
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import time

from driftbench.core.schema.factory import get_schema_extractor
from driftbench.core.data.single_table import SingleTableDriftGenerator


DEFAULT_WORKLOADS = [
    "census_original_uniform_sqls",
    "census_original_skew_sqls",
    "census_original_normal_sqls",
    "census_original_sqls_selectivity_1",
    "census_original_sqls_selectivity_2",
    "census_original_sqls_selectivity_3",
]

DEFAULT_ESTIMATORS = ["postgres"]
DEFAULT_TRAIN_WORKLOAD = "census_original_uniform_sqls"
DEFAULT_NARU_TRAIN_PARAMS = {
    "layers": 4,
    "fc_hiddens": 16,
    "embed_size": 8,
    "input_encoding": "embed",
    "output_encoding": "embed",
    "residual": True,
    "warmups": 0,
    "epochs": 100,
}
DEFAULT_MSCN_TRAIN_PARAMS = {
    "num_samples": 500,
    "hid_units": 8,
    "epochs": 100,
    "bs": 1024,
    "batch": 256,
    "train_num": 100000,
}
DEFAULT_MODEL_TEST_PARAMS = {"num_queries": 2000, "psample": 2000}
DEFAULT_SQL_TABLE_MODE = "versioned"


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Missing env var: {name}")
    return value


def _load_schema(
    schema_path: Path, csv_path: Path, base_table: str, sample_size: int
) -> dict:
    if schema_path.exists():
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
        if base_table in schema.get("tables", {}):
            return schema
    extractor = get_schema_extractor(
        source_type="csv", csv_path=str(csv_path), sample_size=sample_size
    )
    schema = extractor.extract_schema()
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    schema_path.write_text(json.dumps(schema, indent=2), encoding="utf-8")
    return schema


def _ensure_workloads(data_root: Path, dataset: str, workloads: List[str]) -> None:
    missing = []
    for w in workloads:
        p = data_root / dataset / "workload" / f"{w}.pkl"
        if not p.exists():
            missing.append(str(p))
    if missing:
        raise SystemExit("Missing workload pkls:\n  " + "\n  ".join(missing))


def _parse_csv_list(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_float_list(value: str) -> List[float]:
    values = []
    for item in _parse_csv_list(value):
        try:
            values.append(float(item))
        except ValueError as exc:
            raise SystemExit(f"Invalid float value in list: {item}") from exc
    return values


def _infer_skew_value(version: str) -> Optional[float]:
    match = re.search(r"skew[_-]?(\d+(?:\.\d+)?)", version)
    if not match:
        return None
    try:
        val = float(match.group(1))
    except ValueError:
        return None
    return int(val) if val.is_integer() else val


def _write_csv_row(path: Path, row: Dict[str, object], header: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        if not exists:
            writer.writeheader()
        writer.writerow(row)


def _write_sql_plan_file(plan_dir: Path, plan_id: str, statements: List[str]) -> Path:
    plan_dir.mkdir(parents=True, exist_ok=True)
    sql_path = plan_dir / f"{plan_id}.sql"
    sql_path.write_text("\n".join(statements) + "\n", encoding="utf-8")
    return sql_path


def _write_temp_csv_with_row_id(
    df: pd.DataFrame,
    columns: List[str],
    temp_dir: Path,
    prefix: str,
) -> Path:
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_csv = temp_dir / f"{prefix}_{os.getpid()}_{int(dt.datetime.now().timestamp())}.csv"
    data = df[columns].copy()
    data.insert(0, "_row_id", range(1, len(data) + 1))
    data.to_csv(temp_csv, index=False)
    return temp_csv


def _build_update_plan(
    table_name: str,
    temp_name: str,
    columns: List[str],
    update_columns: List[str],
    temp_csv: Path,
) -> List[str]:
    col_list = ", ".join([f"\"{c}\"" for c in columns])
    set_clause = ", ".join([f"\"{c}\" = s.\"{c}\"" for c in update_columns])
    copy_path = temp_csv.resolve().as_posix()
    return [
        f'CREATE TEMP TABLE "{temp_name}" (LIKE "{table_name}" INCLUDING ALL);',
        f'ALTER TABLE "{temp_name}" ADD COLUMN "_row_id" BIGINT;',
        f'COPY "{temp_name}" ("_row_id", {col_list}) FROM \'{copy_path}\' WITH CSV HEADER;',
        f"""
WITH numbered AS (
    SELECT ctid, row_number() OVER (ORDER BY ctid) AS rn
    FROM "{table_name}"
)
UPDATE "{table_name}" t
SET {set_clause}
FROM numbered n
JOIN "{temp_name}" s ON s."_row_id" = n.rn
WHERE t.ctid = n.ctid;
""".strip(),
    ]


def _build_insert_plan(
    table_name: str,
    temp_name: str,
    columns: List[str],
    start_row: int,
    temp_csv: Path,
) -> List[str]:
    col_list = ", ".join([f"\"{c}\"" for c in columns])
    copy_path = temp_csv.resolve().as_posix()
    return [
        f'CREATE TEMP TABLE "{temp_name}" (LIKE "{table_name}" INCLUDING ALL);',
        f'ALTER TABLE "{temp_name}" ADD COLUMN "_row_id" BIGINT;',
        f'COPY "{temp_name}" ("_row_id", {col_list}) FROM \'{copy_path}\' WITH CSV HEADER;',
        f"""
INSERT INTO "{table_name}" ({col_list})
SELECT {col_list}
FROM "{temp_name}"
WHERE "_row_id" > {start_row};
""".strip(),
    ]


def _sql_literal(value: object) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"


def _write_row_update_csv(
    path: Path,
    table_name: str,
    df: pd.DataFrame,
    update_columns: List[str],
    key_column: Optional[str],
    start_ts_ns: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["ts_ns", "sql"])
        ts_ns = start_ts_ns
        for idx, row in df.iterrows():
            assignments = []
            for col in update_columns:
                assignments.append(f"\"{col}\" = {_sql_literal(row[col])}")
            if key_column:
                key_value = row[key_column] if key_column in row else idx + 1
                where_clause = f"\"{key_column}\" = {_sql_literal(key_value)}"
                stmt = f"UPDATE \"{table_name}\" SET {', '.join(assignments)} WHERE {where_clause};"
            else:
                stmt = (
                    f"UPDATE \"{table_name}\" t "
                    f"SET {', '.join(assignments)} "
                    f"FROM (SELECT ctid, row_number() OVER (ORDER BY ctid) AS rn "
                    f"FROM \"{table_name}\") n "
                    f"WHERE t.ctid = n.ctid AND n.rn = {idx + 1};"
                )
            writer.writerow([ts_ns, stmt])
            ts_ns += 1


def _resolve_static_skew_sources(
    skew_versions: List[str],
    skew_csvs: List[str],
    data_root: Path,
    dataset: str,
) -> List[tuple[str, Path]]:
    entries: List[tuple[str, Path]] = []
    for version in skew_versions:
        csv_path = data_root / dataset / f"{version}.csv"
        if not csv_path.exists():
            raise SystemExit(f"Skew version CSV missing: {csv_path}")
        entries.append((version, csv_path))

    for csv in skew_csvs:
        src = Path(csv).expanduser()
        if not src.exists():
            raise SystemExit(f"Skew CSV not found: {src}")
        version = src.stem
        dest = data_root / dataset / f"{version}.csv"
        if src.resolve() != dest.resolve():
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, dest)
        entries.append((version, dest))
    return entries


def _find_latest_model_name(model_root: Path, prefix: str) -> Optional[str]:
    if not model_root.exists():
        return None
    candidates = sorted(
        model_root.glob(f"{prefix}*.pt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        return None
    return candidates[0].stem


def _ensure_model_labels(
    lecarb_root: Path,
    dataset: str,
    version: str,
    workload: str,
    log_dir: Path,
) -> None:
    _run_cmd(
        [
            sys.executable,
            "-m",
            "lecarb",
            "workload",
            "label",
            "-d",
            dataset,
            "-v",
            version,
            "-w",
            workload,
        ],
        log_dir / f"train_label_{version}_{workload}.log",
        cwd=lecarb_root,
    )


def _ensure_naru_model(
    lecarb_root: Path,
    output_root: Path,
    dataset: str,
    base_version: str,
    workload: str,
    seed: int,
    log_dir: Path,
    force_train: bool,
) -> str:
    model_root = output_root / "model" / dataset
    prefix = f"{base_version}-"
    model_name = _find_latest_model_name(model_root, prefix)
    if model_name and not force_train:
        return model_name

    _ensure_model_labels(lecarb_root, dataset, base_version, workload, log_dir)
    _run_cmd(
        [
            sys.executable,
            "-m",
            "lecarb",
            "train",
            "-s",
            str(seed),
            "-d",
            dataset,
            "-v",
            base_version,
            "-w",
            workload,
            "-e",
            "naru",
            "--params",
            str(DEFAULT_NARU_TRAIN_PARAMS),
            "--sizelimit",
            "0",
        ],
        log_dir / f"train_naru_{base_version}.log",
        cwd=lecarb_root,
    )
    model_name = _find_latest_model_name(model_root, prefix)
    if not model_name:
        raise SystemExit(f"Naru model not found under {model_root}")
    return model_name


def _ensure_mscn_model(
    lecarb_root: Path,
    output_root: Path,
    dataset: str,
    base_version: str,
    workload: str,
    seed: int,
    log_dir: Path,
    force_train: bool,
) -> str:
    model_root = output_root / "model" / dataset
    prefix = f"{base_version}_{workload}-"
    model_name = _find_latest_model_name(model_root, prefix)
    if model_name and not force_train:
        return model_name

    _ensure_model_labels(lecarb_root, dataset, base_version, workload, log_dir)
    _run_cmd(
        [
            sys.executable,
            "-m",
            "lecarb",
            "train",
            "-s",
            str(seed),
            "-d",
            dataset,
            "-v",
            base_version,
            "-w",
            workload,
            "-e",
            "mscn",
            "--params",
            str(DEFAULT_MSCN_TRAIN_PARAMS),
            "--sizelimit",
            "0",
        ],
        log_dir / f"train_mscn_{base_version}.log",
        cwd=lecarb_root,
    )
    model_name = _find_latest_model_name(model_root, prefix)
    if not model_name:
        raise SystemExit(f"MSCN model not found under {model_root}")
    return model_name


def _ensure_models(
    lecarb_root: Path,
    output_root: Path,
    dataset: str,
    base_version: str,
    train_workload: str,
    estimators: List[str],
    seed: int,
    log_dir: Path,
    force_train: bool,
) -> Dict[str, str]:
    models: Dict[str, str] = {}
    if "naru" in estimators:
        models["naru"] = _ensure_naru_model(
            lecarb_root,
            output_root,
            dataset,
            base_version,
            train_workload,
            seed,
            log_dir,
            force_train,
        )
    if "mscn" in estimators:
        models["mscn"] = _ensure_mscn_model(
            lecarb_root,
            output_root,
            dataset,
            base_version,
            train_workload,
            seed,
            log_dir,
            force_train,
        )
    return models


def _test_params_for_estimator(
    estimator: str,
    version: str,
    stat_target: int,
    models: Dict[str, str],
    stat_version: Optional[str] = None,
) -> Dict[str, object]:
    if estimator == "postgres":
        return {"version": stat_version or version, "stat_target": stat_target}
    if estimator == "naru":
        return {"model": models["naru"], **DEFAULT_MODEL_TEST_PARAMS}
    if estimator == "mscn":
        return {"model": models["mscn"], **DEFAULT_MODEL_TEST_PARAMS}
    raise ValueError(f"Unsupported estimator: {estimator}")

def _connect_pg():
    try:
        import psycopg2  # type: ignore
    except Exception as exc:
        raise SystemExit(f"psycopg2 is required for postgres checks: {exc}")
    return psycopg2.connect(_require_env("DATABASE_URL"))


def _pg_table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = %s",
            (table_name,),
        )
        return cur.fetchone() is not None


def _get_table_columns(conn, table_name: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def _copy_table(conn, src_table: str, dest_table: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{dest_table}"')
        cur.execute(f'CREATE TABLE "{dest_table}" (LIKE "{src_table}" INCLUDING ALL)')
        cur.execute(f'INSERT INTO "{dest_table}" SELECT * FROM "{src_table}"')
    conn.commit()


def _infer_pg_type(series: pd.Series) -> str:
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE PRECISION"
    return "TEXT"


def _load_csv_to_postgres(conn, csv_path: Path, table_name: str) -> None:
    df_head = pd.read_csv(csv_path, nrows=1000)
    columns = list(df_head.columns)
    col_list = ", ".join([f"\"{c}\"" for c in columns])
    col_defs = []
    for col in columns:
        col_defs.append(f"\"{col}\" {_infer_pg_type(df_head[col])}")

    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cur.execute(f'CREATE TABLE "{table_name}" ({", ".join(col_defs)})')
        with csv_path.open("r", encoding="utf-8") as f:
            cur.copy_expert(
                f'COPY "{table_name}" ({col_list}) FROM STDIN WITH CSV HEADER',
                f,
            )
    conn.commit()


def _load_temp_table_with_row_id(
    conn, table_name: str, df: pd.DataFrame, temp_dir: Path
) -> tuple[str, List[str], Path]:
    columns = _get_table_columns(conn, table_name)
    for col in columns:
        if col not in df.columns:
            raise ValueError(f"Missing column '{col}' in target CSV for {table_name}")
    df = df[columns].copy()
    df.insert(0, "_row_id", range(1, len(df) + 1))

    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_csv = temp_dir / f"tmp_{table_name}_{os.getpid()}.csv"
    df.to_csv(temp_csv, index=False)

    temp_name = f"tmp_update_{os.getpid()}_{int(dt.datetime.now().timestamp())}"
    col_list = ", ".join([f"\"{c}\"" for c in columns])

    with conn.cursor() as cur:
        cur.execute(f'CREATE TEMP TABLE "{temp_name}" (LIKE "{table_name}" INCLUDING ALL)')
        cur.execute(f'ALTER TABLE "{temp_name}" ADD COLUMN "_row_id" BIGINT')
        with temp_csv.open("r", encoding="utf-8") as f:
            cur.copy_expert(
                f'COPY "{temp_name}" ("_row_id", {col_list}) FROM STDIN WITH CSV HEADER',
                f,
            )
    conn.commit()
    return temp_name, columns, temp_csv


def _apply_sql_update(
    conn,
    table_name: str,
    df: pd.DataFrame,
    update_columns: List[str],
    temp_dir: Path,
    plan_dir: Optional[Path] = None,
    plan_id: Optional[str] = None,
) -> Optional[tuple[Path, List[str]]]:
    temp_name, columns, temp_csv = _load_temp_table_with_row_id(
        conn, table_name, df, temp_dir
    )
    if not update_columns:
        update_columns = columns
    for col in update_columns:
        if col not in columns:
            raise ValueError(f"Column '{col}' not found in table '{table_name}'")
    set_clause = ", ".join([f"\"{c}\" = s.\"{c}\"" for c in update_columns])
    col_list = ", ".join([f"\"{c}\"" for c in columns])
    copy_path = temp_csv.resolve().as_posix()
    statements = [
        f'CREATE TEMP TABLE "{temp_name}" (LIKE "{table_name}" INCLUDING ALL);',
        f'ALTER TABLE "{temp_name}" ADD COLUMN "_row_id" BIGINT;',
        f'COPY "{temp_name}" ("_row_id", {col_list}) FROM \'{copy_path}\' WITH CSV HEADER;',
        f"""
WITH numbered AS (
    SELECT ctid, row_number() OVER (ORDER BY ctid) AS rn
    FROM "{table_name}"
)
UPDATE "{table_name}" t
SET {set_clause}
FROM numbered n
JOIN "{temp_name}" s ON s."_row_id" = n.rn
WHERE t.ctid = n.ctid;
""".strip(),
    ]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH numbered AS (
                SELECT ctid, row_number() OVER (ORDER BY ctid) AS rn
                FROM "{table_name}"
            )
            UPDATE "{table_name}" t
            SET {set_clause}
            FROM numbered n
            JOIN "{temp_name}" s ON s."_row_id" = n.rn
            WHERE t.ctid = n.ctid
            """
        )
    conn.commit()
    if plan_dir and plan_id:
        sql_path = _write_sql_plan_file(plan_dir, f"{plan_id}_update", statements)
        return sql_path, statements
    return None


def _apply_sql_insert(
    conn,
    table_name: str,
    df: pd.DataFrame,
    start_row: int,
    temp_dir: Path,
    plan_dir: Optional[Path] = None,
    plan_id: Optional[str] = None,
) -> Optional[tuple[Path, List[str]]]:
    temp_name, columns, temp_csv = _load_temp_table_with_row_id(
        conn, table_name, df, temp_dir
    )
    col_list = ", ".join([f"\"{c}\"" for c in columns])
    copy_path = temp_csv.resolve().as_posix()
    statements = [
        f'CREATE TEMP TABLE "{temp_name}" (LIKE "{table_name}" INCLUDING ALL);',
        f'ALTER TABLE "{temp_name}" ADD COLUMN "_row_id" BIGINT;',
        f'COPY "{temp_name}" ("_row_id", {col_list}) FROM \'{copy_path}\' WITH CSV HEADER;',
        f"""
INSERT INTO "{table_name}" ({col_list})
SELECT {col_list}
FROM "{temp_name}"
WHERE "_row_id" > {start_row};
""".strip(),
    ]

    with conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO "{table_name}" ({col_list})
            SELECT {col_list}
            FROM "{temp_name}"
            WHERE "_row_id" > %s
            """,
            (start_row,),
        )
    conn.commit()
    if plan_dir and plan_id:
        sql_path = _write_sql_plan_file(plan_dir, f"{plan_id}_insert", statements)
        return sql_path, statements
    return None


def _run_cmd(cmd: List[str], log_path: Path, cwd: Optional[Path] = None) -> None:
    import subprocess

    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"[{dt.datetime.now().isoformat()}] RUN: {' '.join(cmd)}\n")
        f.flush()
        proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=False, cwd=cwd)
        if proc.returncode != 0:
            raise SystemExit(f"Command failed: {' '.join(cmd)} (exit {proc.returncode})")


def _workloads_for_minute(
    minute: int,
    default: List[str],
    schedule: Optional[Dict[str, List[str]]],
) -> List[str]:
    if not schedule:
        return default
    if str(minute) in schedule:
        return schedule[str(minute)]
    if "default" in schedule:
        return schedule["default"]
    return default


def _write_timeline_row(
    timeline_path: Path,
    row: Dict[str, object],
    header: Optional[List[str]] = None,
) -> None:
    exists = timeline_path.exists()
    timeline_path.parent.mkdir(parents=True, exist_ok=True)
    columns = header or list(row.keys())
    with timeline_path.open("a", encoding="utf-8") as f:
        if not exists:
            f.write(",".join(columns) + "\n")
        f.write(",".join(str(row[c]) for c in columns) + "\n")


def run_case(
    case_name: str,
    base_csv: Path,
    base_table: str,
    schema: dict,
    data_root: Path,
    dataset: str,
    minutes: int,
    workloads: List[str],
    workload_schedule: Optional[Dict[str, List[str]]],
    log_dir: Path,
    pg_conn,
    stat_target: int,
    growth_per_min: float,
    growth_mode: str,
    skew_columns: List[str],
    skew_start: int,
    skew_end: int,
    skew_mode: str,
    skew_step: float,
    skew_static: List[tuple[str, Path]],
    skew_values: List[float],
    skew_repeat_last: bool,
    sql_plan_dir: Optional[Path],
    sql_table_mode: str,
    sql_plan_format: str,
    sql_target_table: Optional[str],
    sql_key_column: Optional[str],
    sql_plan_only: bool,
    db_apply_mode: str,
    sql_update_columns: List[str],
    temp_dir: Path,
    estimators: List[str],
    models: Dict[str, str],
    seed: int,
    lecarb_root: Path,
    skip_tests: bool,
) -> None:
    gen = SingleTableDriftGenerator(str(base_csv), schema, base_table, seed=seed)
    base_df = gen.df.copy()
    base_rows = len(base_df)
    skew_df = base_df.copy()
    cardinality_df = base_df.copy()
    current_rows = base_rows
    sql_plan_path = sql_plan_dir / "sql_plan.csv" if sql_plan_dir else None
    sql_plan_header = [
        "minute",
        "timestamp",
        "case",
        "version",
        "table",
        "op",
        "sql_file",
        "sql",
    ]

    for minute in range(minutes):
        timestamp = dt.datetime.now().isoformat()

        if case_name == "cardinality":
            version = f"original_{case_name}_t{minute:02d}"
            version_csv = data_root / dataset / f"{version}.csv"
            if growth_mode == "incremental" and minute > 0:
                target_rows = int(round(current_rows * (1 + growth_per_min)))
                if target_rows <= current_rows:
                    df = cardinality_df.copy()
                else:
                    gen.df = cardinality_df.copy()
                    new_rows = gen._generate_rows_like_existing(target_rows - current_rows)
                    df = pd.concat([cardinality_df, new_rows], ignore_index=True)
                cardinality_df = df
            else:
                target_rows = int(round(base_rows * (1 + growth_per_min * minute)))
                if target_rows <= base_rows:
                    df = base_df.copy()
                else:
                    gen.df = base_df
                    new_rows = gen._generate_rows_like_existing(target_rows - base_rows)
                    df = pd.concat([base_df, new_rows], ignore_index=True)
        elif case_name == "skew":
            if skew_static:
                if minute >= len(skew_static):
                    if skew_repeat_last:
                        idx = len(skew_static) - 1
                    else:
                        raise SystemExit(
                            f"Not enough skew datasets for minute {minute}: "
                            f"have {len(skew_static)} entries"
                        )
                else:
                    idx = minute
                version, version_csv = skew_static[idx]
                df = pd.read_csv(version_csv)
            else:
                version = f"original_{case_name}_t{minute:02d}"
                version_csv = data_root / dataset / f"{version}.csv"
                skew_target = min(skew_start + minute, skew_end)
                if skew_mode == "incremental" and minute > 0:
                    gen.df = skew_df.copy()
                    df = gen._inject_skew(
                        columns=skew_columns, portion=1.0, skewness=skew_step
                    )
                else:
                    gen.df = base_df.copy()
                    df = gen._inject_skew(
                        columns=skew_columns, portion=1.0, skewness=skew_target
                    )
                skew_df = df
        else:
            raise ValueError(f"Unsupported case: {case_name}")

        version_csv.parent.mkdir(parents=True, exist_ok=True)
        if case_name == "cardinality" or not skew_static:
            df.to_csv(version_csv, index=False)

        use_inplace = db_apply_mode == "sql" and sql_table_mode == "inplace"
        table_name = (
            f"{dataset}_{base_table}" if use_inplace else f"{dataset}_{version}"
        )
        sql_table_name = sql_target_table or table_name
        base_table_name = f"{dataset}_{base_table}"
        if db_apply_mode == "sql":
            apply_sql = True
            if use_inplace:
                if not sql_plan_only:
                    if not _pg_table_exists(pg_conn, base_table_name):
                        raise SystemExit(f"Base table missing: {base_table_name}")
            else:
                if not sql_plan_only:
                    if not _pg_table_exists(pg_conn, table_name):
                        if not _pg_table_exists(pg_conn, base_table_name):
                            raise SystemExit(f"Base table missing: {base_table_name}")
                        _copy_table(pg_conn, base_table_name, table_name)
                    else:
                        apply_sql = False

            if apply_sql:
                plan_id = (
                    f"{dataset}_{version}"
                    if case_name == "skew" and skew_static
                    else f"{case_name}_{minute:02d}"
                )
                if case_name == "skew":
                    update_cols = sql_update_columns or skew_columns
                    if sql_plan_only:
                        columns = list(df.columns)
                        for col in update_cols:
                            if col not in columns:
                                raise ValueError(
                                    f"Column '{col}' not found in update data"
                                )
                        if sql_plan_format == "row_update":
                            if not sql_plan_dir:
                                raise ValueError("--sql-plan-dir is required for row_update")
                            csv_path = sql_plan_dir / f"{plan_id}_updates.csv"
                            _write_row_update_csv(
                                csv_path,
                                sql_table_name,
                                df,
                                update_cols,
                                sql_key_column,
                                start_ts_ns=time.time_ns(),
                            )
                        else:
                            temp_csv = _write_temp_csv_with_row_id(
                                df, columns, temp_dir, f"tmp_update_{plan_id}"
                            )
                            statements = _build_update_plan(
                                sql_table_name,
                                f"tmp_update_{plan_id}",
                                columns,
                                update_cols,
                                temp_csv,
                            )
                            sql_path = (
                                _write_sql_plan_file(sql_plan_dir, f"{plan_id}_update", statements)
                                if sql_plan_dir
                                else None
                            )
                            if sql_plan_path:
                                sql_text = " ".join(
                                    s.strip().replace("\n", " ") for s in statements
                                )
                                _write_csv_row(
                                    sql_plan_path,
                                    {
                                        "minute": minute,
                                        "timestamp": timestamp,
                                        "case": case_name,
                                        "version": version,
                                        "table": table_name,
                                        "op": "update",
                                        "sql_file": str(sql_path) if sql_path else "",
                                        "sql": sql_text,
                                    },
                                    sql_plan_header,
                                )
                    else:
                        plan = _apply_sql_update(
                            pg_conn,
                            table_name,
                            df,
                            update_cols,
                            temp_dir,
                            plan_dir=sql_plan_dir,
                            plan_id=plan_id,
                        )
                        if plan and sql_plan_path:
                            sql_path, statements = plan
                            sql_text = " ".join(
                                s.strip().replace("\n", " ") for s in statements
                            )
                            _write_csv_row(
                                sql_plan_path,
                                {
                                    "minute": minute,
                                    "timestamp": timestamp,
                                    "case": case_name,
                                    "version": version,
                                    "table": table_name,
                                    "op": "update",
                                    "sql_file": str(sql_path),
                                    "sql": sql_text,
                                },
                                sql_plan_header,
                            )
                elif case_name == "cardinality":
                    insert_from = current_rows if use_inplace else base_rows
                    if sql_plan_only:
                        columns = list(df.columns)
                        temp_csv = _write_temp_csv_with_row_id(
                            df, columns, temp_dir, f"tmp_insert_{plan_id}"
                        )
                        statements = _build_insert_plan(
                            sql_table_name,
                            f"tmp_insert_{plan_id}",
                            columns,
                            insert_from,
                            temp_csv,
                        )
                        if use_inplace:
                            current_rows = len(df)
                        sql_path = (
                            _write_sql_plan_file(sql_plan_dir, f"{plan_id}_insert", statements)
                            if sql_plan_dir
                            else None
                        )
                        if sql_plan_path:
                            sql_text = " ".join(
                                s.strip().replace("\n", " ") for s in statements
                            )
                            _write_csv_row(
                                sql_plan_path,
                                {
                                    "minute": minute,
                                    "timestamp": timestamp,
                                    "case": case_name,
                                    "version": version,
                                    "table": table_name,
                                    "op": "insert",
                                    "sql_file": str(sql_path) if sql_path else "",
                                    "sql": sql_text,
                                },
                                sql_plan_header,
                            )
                    else:
                        plan = _apply_sql_insert(
                            pg_conn,
                            table_name,
                            df,
                            insert_from,
                            temp_dir,
                            plan_dir=sql_plan_dir,
                            plan_id=plan_id,
                        )
                        if use_inplace:
                            current_rows = len(df)
                        if plan and sql_plan_path:
                            sql_path, statements = plan
                            sql_text = " ".join(
                                s.strip().replace("\n", " ") for s in statements
                            )
                            _write_csv_row(
                                sql_plan_path,
                                {
                                    "minute": minute,
                                    "timestamp": timestamp,
                                    "case": case_name,
                                    "version": version,
                                    "table": table_name,
                                    "op": "insert",
                                    "sql_file": str(sql_path),
                                    "sql": sql_text,
                                },
                                sql_plan_header,
                            )
        else:
            if not _pg_table_exists(pg_conn, table_name):
                _load_csv_to_postgres(pg_conn, version_csv, table_name)

        if skip_tests:
            continue

        workloads_min = _workloads_for_minute(minute, workloads, workload_schedule)

        timeline_path = log_dir / f"timeline_{case_name}.csv"
        for workload in workloads_min:
            _run_cmd(
                [
                    sys.executable,
                    "-m",
                    "lecarb",
                    "workload",
                    "label",
                    "-d",
                    dataset,
                    "-v",
                    version,
                    "-w",
                    workload,
                ],
                log_dir / f"{case_name}_{version}_{workload}_label.log",
                cwd=lecarb_root,
            )

            stat_version = base_table if use_inplace else version
            for estimator in estimators:
                params = _test_params_for_estimator(
                    estimator, version, stat_target, models, stat_version=stat_version
                )
                _run_cmd(
                    [
                        sys.executable,
                        "-m",
                        "lecarb",
                        "test",
                        "-s",
                        str(seed),
                        "-d",
                        dataset,
                        "-v",
                        version,
                        "-w",
                        workload,
                        "-e",
                        estimator,
                        "--params",
                        str(params),
                        "--overwrite",
                    ],
                    log_dir / f"{case_name}_{version}_{workload}_{estimator}.log",
                    cwd=lecarb_root,
                )

                row = {
                    "minute": minute,
                    "timestamp": timestamp,
                    "case": case_name,
                    "version": version,
                    "rows": len(df),
                    "workload": workload,
                    "estimator": estimator,
                }
                if case_name == "cardinality":
                    row["scale"] = round(len(df) / base_rows, 3)
                else:
                    if skew_static:
                        skew_val = (
                            skew_values[idx]
                            if idx < len(skew_values)
                            else _infer_skew_value(version)
                        )
                        row["skewness"] = skew_val if skew_val is not None else ""
                        row["skew_mode"] = "static"
                        row["skew_step"] = ""
                    else:
                        row["skewness"] = min(skew_start + minute, skew_end)
                        row["skew_mode"] = skew_mode
                        row["skew_step"] = skew_step
                    row["skew_columns"] = "|".join(skew_columns)
                row["db_apply_mode"] = db_apply_mode
                row["sql_table_mode"] = sql_table_mode
                row["model"] = models.get(estimator, "")
                _write_timeline_row(timeline_path, row)


def main() -> None:
    parser = argparse.ArgumentParser(description="Census CE timeline runner")
    parser.add_argument("--minutes", type=int, default=10)
    parser.add_argument("--cases", default="cardinality,skew")
    parser.add_argument("--dataset", default="census13")
    parser.add_argument("--base-version", default="original")
    parser.add_argument("--schema-path", default="")
    parser.add_argument("--schema-sample-size", type=int, default=1000)
    parser.add_argument("--workloads", default=",".join(DEFAULT_WORKLOADS))
    parser.add_argument("--workload-schedule", default="")
    parser.add_argument(
        "--estimators",
        default=os.environ.get("CE_ESTIMATORS", ",".join(DEFAULT_ESTIMATORS)),
    )
    parser.add_argument(
        "--train-workload",
        default=os.environ.get("CE_TRAIN_WORKLOAD", DEFAULT_TRAIN_WORKLOAD),
    )
    parser.add_argument(
        "--force-train-models",
        action="store_true",
        default=os.environ.get("CE_FORCE_TRAIN", "0") == "1",
    )
    parser.add_argument("--growth-per-min", type=float, default=0.1)
    parser.add_argument(
        "--growth-mode",
        choices=["base", "incremental"],
        default=os.environ.get("CARDINALITY_GROWTH_MODE", "base"),
    )
    parser.add_argument("--skew-columns", default="age")
    parser.add_argument("--skew-start", type=int, default=2)
    parser.add_argument("--skew-end", type=int, default=9)
    parser.add_argument("--skew-versions", default="")
    parser.add_argument("--skew-csvs", default="")
    parser.add_argument("--skew-values", default="")
    parser.add_argument("--skew-repeat-last", action="store_true")
    parser.add_argument(
        "--skew-mode",
        choices=["base", "incremental"],
        default="base",
        help="base: derive each minute from base; incremental: apply step to previous minute",
    )
    parser.add_argument("--skew-step", type=float, default=1.0)
    parser.add_argument(
        "--sql-plan-dir",
        default=os.environ.get("SQL_PLAN_DIR", ""),
    )
    parser.add_argument(
        "--sql-plan-format",
        choices=["temp_table", "row_update"],
        default=os.environ.get("SQL_PLAN_FORMAT", "temp_table"),
    )
    parser.add_argument(
        "--sql-target-table",
        default=os.environ.get("SQL_TARGET_TABLE", ""),
    )
    parser.add_argument(
        "--sql-key-column",
        default=os.environ.get("SQL_KEY_COLUMN", ""),
    )
    parser.add_argument(
        "--sql-plan-only",
        action="store_true",
        default=os.environ.get("SQL_PLAN_ONLY", "0") == "1",
    )
    parser.add_argument(
        "--sql-table-mode",
        choices=["versioned", "inplace"],
        default=os.environ.get("SQL_TABLE_MODE", DEFAULT_SQL_TABLE_MODE),
    )
    parser.add_argument(
        "--db-apply-mode",
        choices=["reload", "sql"],
        default=os.environ.get("DB_APPLY_MODE", "reload"),
    )
    parser.add_argument("--sql-update-columns", default="")
    parser.add_argument("--stat-target", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=123)
    args = parser.parse_args()

    if args.sql_plan_only and args.db_apply_mode != "sql":
        raise SystemExit("--sql-plan-only requires --db-apply-mode sql")

    data_root = Path(_require_env("DATA_ROOT"))
    output_root = Path(_require_env("OUTPUT_ROOT"))
    if not args.sql_plan_only:
        _require_env("DATABASE_URL")

    base_csv = data_root / args.dataset / f"{args.base_version}.csv"
    if not base_csv.exists():
        raise SystemExit(f"Base CSV missing: {base_csv}")

    base_table = args.base_version
    schema_path = Path(
        args.schema_path
        or f"./output/intermediate_yaml/{args.dataset}_{args.base_version}_schema.json"
    )
    schema = _load_schema(schema_path, base_csv, base_table, args.schema_sample_size)

    workloads = _parse_csv_list(args.workloads)
    train_workload = args.train_workload.strip()
    workloads_to_check = list(workloads)
    if train_workload and train_workload not in workloads_to_check:
        workloads_to_check.append(train_workload)
    if not args.sql_plan_only:
        _ensure_workloads(data_root, args.dataset, workloads_to_check)

    estimators = [e.lower() for e in _parse_csv_list(args.estimators)]
    if not estimators:
        estimators = list(DEFAULT_ESTIMATORS)
    allowed_estimators = {"postgres", "naru", "mscn"}
    unknown = [e for e in estimators if e not in allowed_estimators]
    if unknown:
        raise SystemExit(f"Unsupported estimators: {', '.join(unknown)}")

    workload_schedule = None
    if args.workload_schedule:
        schedule_path = Path(args.workload_schedule)
        workload_schedule = json.loads(schedule_path.read_text(encoding="utf-8"))

    conn = None
    if not args.sql_plan_only:
        conn = _connect_pg()
        if not _pg_table_exists(conn, f"{args.dataset}_{args.base_version}"):
            raise SystemExit(
                f"Base table missing in Postgres: {args.dataset}_{args.base_version}"
            )

    repo_root = Path(__file__).resolve().parents[2]
    lecarb_root = repo_root / "existing_benchmarks" / "AreCELearnedYet"
    if not (lecarb_root / "lecarb").exists():
        raise SystemExit(f"lecarb package not found at: {lecarb_root}")

    log_dir = output_root / "log" / "ce_timeline"
    temp_dir = log_dir / "tmp_sql"
    cases = [c.strip() for c in args.cases.split(",") if c.strip()]
    skew_columns = [c.strip() for c in args.skew_columns.split(",") if c.strip()]
    sql_update_columns = [
        c.strip() for c in args.sql_update_columns.split(",") if c.strip()
    ]
    skew_versions = _parse_csv_list(args.skew_versions)
    skew_csvs = _parse_csv_list(args.skew_csvs)
    skew_values = _parse_float_list(args.skew_values)
    skew_static = (
        _resolve_static_skew_sources(skew_versions, skew_csvs, data_root, args.dataset)
        if (skew_versions or skew_csvs)
        else []
    )
    sql_plan_dir = Path(args.sql_plan_dir).expanduser() if args.sql_plan_dir else None
    skip_tests = args.sql_plan_only
    models: Dict[str, str] = {}
    if not skip_tests:
        models = _ensure_models(
            lecarb_root=lecarb_root,
            output_root=output_root,
            dataset=args.dataset,
            base_version=args.base_version,
            train_workload=train_workload,
            estimators=estimators,
            seed=args.seed,
            log_dir=log_dir,
            force_train=args.force_train_models,
        )

    for case in cases:
        run_case(
            case_name=case,
            base_csv=base_csv,
            base_table=base_table,
            schema=schema,
            data_root=data_root,
            dataset=args.dataset,
            minutes=args.minutes,
            workloads=workloads,
            workload_schedule=workload_schedule,
            log_dir=log_dir,
            pg_conn=conn,
            stat_target=args.stat_target,
            growth_per_min=args.growth_per_min,
            growth_mode=args.growth_mode,
            skew_columns=skew_columns,
            skew_start=args.skew_start,
            skew_end=args.skew_end,
            skew_mode=args.skew_mode,
            skew_step=args.skew_step,
            skew_static=skew_static,
            skew_values=skew_values,
            skew_repeat_last=args.skew_repeat_last,
            sql_plan_dir=sql_plan_dir,
            sql_table_mode=args.sql_table_mode,
            sql_plan_format=args.sql_plan_format,
            sql_target_table=args.sql_target_table or None,
            sql_key_column=args.sql_key_column or None,
            sql_plan_only=args.sql_plan_only,
            db_apply_mode=args.db_apply_mode,
            sql_update_columns=sql_update_columns,
            temp_dir=temp_dir,
            estimators=estimators,
            models=models,
            seed=args.seed,
            lecarb_root=lecarb_root,
            skip_tests=skip_tests,
        )

    if conn is not None:
        conn.close()


if __name__ == "__main__":
    main()

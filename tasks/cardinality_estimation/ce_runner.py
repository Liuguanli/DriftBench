#!/usr/bin/env python3
import argparse
import csv
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd

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


def _load_env(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def _load_schema(
    schema_path: Path,
    csv_path: Path,
    base_table: str,
    sample_size: int = 1000,
) -> dict:
    from driftbench.core.schema.factory import get_schema_extractor

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


def _log_step(log_dir: Path, message: str) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    with (log_dir / "run_steps.log").open("a", encoding="utf-8") as f:
        f.write(f"[{pd.Timestamp.now().isoformat()}] {message}\n")


def _run_cmd(cmd: List[str], log_path: Path, cwd: Path | None = None) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as f:
        proc = subprocess.run(cmd, cwd=cwd, stdout=f, stderr=subprocess.STDOUT)
    if proc.returncode != 0:
        raise SystemExit(f"Command failed: {' '.join(cmd)} (see {log_path})")


def _ensure_row_id_csv(csv_path: Path) -> None:
    df = pd.read_csv(csv_path)
    if "row_id" not in df.columns:
        df.insert(0, "row_id", range(1, len(df) + 1))
        df.to_csv(csv_path, index=False)


def _parse_csv_list(value: str) -> List[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


def _parse_float_list(value: str) -> List[float]:
    values = []
    for item in _parse_csv_list(value):
        try:
            values.append(float(item))
        except ValueError as exc:
            raise SystemExit(f"Invalid float value: {item}") from exc
    return values


def _resolve_workload_names(items: Iterable[str]) -> List[str]:
    names = []
    for item in items:
        p = Path(item)
        if p.exists():
            names.append(p.stem)
        else:
            names.append(item)
    return names


def _sort_skew_files(files: List[Path]) -> List[Path]:
    def key(path: Path):
        m = re.search(r"skew_(\d+)", path.name)
        if m:
            return (0, int(m.group(1)), path.name)
        return (1, path.name)

    return sorted(files, key=key)


def _collect_update_files(paths: List[str], plan_dir: Path) -> List[Path]:
    files: List[Path] = []
    if not paths:
        files.extend(plan_dir.glob("*_updates.csv"))
        return _sort_skew_files(files)

    for item in paths:
        for part in _parse_csv_list(item):
            p = Path(part)
            if p.is_dir():
                files.extend(p.glob("*_updates.csv"))
            else:
                files.append(p)
    return _sort_skew_files(files)


def _check_env(require_db: bool, require_lecarb: bool, require_base_csv: bool) -> None:
    if require_db and not os.environ.get("DATABASE_URL"):
        raise SystemExit("DATABASE_URL is not set.")
    if require_base_csv:
        base_csv = Path(os.environ.get("BASE_CSV", "data/census_original.csv"))
        if not base_csv.exists():
            raise SystemExit(f"Base CSV missing: {base_csv}")
    if require_lecarb:
        lecarb_root = Path(os.environ["REPO_ROOT"]) / "existing_benchmarks" / "AreCELearnedYet"
        if not (lecarb_root / "lecarb").exists():
            raise SystemExit(f"lecarb not found at {lecarb_root}")


def _connect_pg():
    try:
        import psycopg2  # type: ignore
    except Exception as exc:
        raise SystemExit(f"psycopg2 is required for postgres checks: {exc}")
    return psycopg2.connect(os.environ["DATABASE_URL"])


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


def _append_rows_to_postgres(
    conn,
    table_name: str,
    df: pd.DataFrame,
    start_row: int,
    temp_dir: Path,
) -> int:
    if start_row >= len(df):
        return 0
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_csv = temp_dir / f"tmp_insert_{table_name}_{os.getpid()}_{int(time.time())}.csv"
    new_rows = df.iloc[start_row:].copy()
    columns = list(new_rows.columns)
    col_list = ", ".join([f"\"{c}\"" for c in columns])
    new_rows.to_csv(temp_csv, index=False)

    with conn.cursor() as cur:
        with temp_csv.open("r", encoding="utf-8") as f:
            cur.copy_expert(
                f'COPY "{table_name}" ({col_list}) FROM STDIN WITH CSV HEADER',
                f,
            )
    conn.commit()
    return len(new_rows)


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


def _load_model_lock(path: Path) -> dict:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_model_lock(path: Path, lock: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(lock, f, indent=2)


def _ensure_model_lock(
    dataset: str,
    base_version: str,
    train_workload: str,
    estimators: List[str],
    seed: int,
    log_dir: Path,
    lecarb_root: Path,
    model_lock_path: Path,
) -> dict:
    lock = _load_model_lock(model_lock_path)
    entry = lock.get(dataset, {})

    model_root = Path(os.environ.get("OUTPUT_ROOT", ".")) / "model" / dataset
    naru_prefix = f"{base_version}-"
    mscn_prefix = f"{base_version}_{train_workload}-"

    _ensure_dataset_artifacts(dataset, base_version, log_dir)
    _ensure_workloads(
        dataset,
        base_version,
        [train_workload],
        seed,
        log_dir,
        lecarb_root,
    )

    if entry.get("naru"):
        naru_path = model_root / f"{entry['naru']}.pt"
        if not naru_path.exists():
            _log_step(log_dir, f"naru_model_missing: {naru_path}")
            entry["naru"] = None
    if not entry.get("naru"):
        existing_naru = _find_latest_model_name(model_root, naru_prefix)
        if existing_naru:
            entry["naru"] = existing_naru
            _log_step(log_dir, f"naru_model_exists: dataset={dataset} model={existing_naru}")

    if "naru" in estimators and not entry.get("naru"):
        _log_step(log_dir, f"train_naru: dataset={dataset} workload={train_workload}")
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
                train_workload,
                "-e",
                "naru",
                "--params",
                repr(DEFAULT_NARU_TRAIN_PARAMS),
                "--sizelimit",
                "0",
            ],
            log_dir / f"train_{dataset}_naru.log",
            cwd=lecarb_root,
        )
        entry["naru"] = _find_latest_model_name(model_root, naru_prefix)
        if not entry["naru"]:
            raise SystemExit(f"Naru model not found under {model_root}")

    if entry.get("mscn"):
        mscn_path = model_root / f"{entry['mscn']}.pt"
        if not mscn_path.exists():
            _log_step(log_dir, f"mscn_model_missing: {mscn_path}")
            entry["mscn"] = None
    if not entry.get("mscn"):
        existing_mscn = _find_latest_model_name(model_root, mscn_prefix)
        if existing_mscn:
            entry["mscn"] = existing_mscn
            _log_step(log_dir, f"mscn_model_exists: dataset={dataset} model={existing_mscn}")

    if "mscn" in estimators and not entry.get("mscn"):
        _log_step(log_dir, f"train_mscn: dataset={dataset} workload={train_workload}")
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
                train_workload,
                "-e",
                "mscn",
                "--params",
                repr(DEFAULT_MSCN_TRAIN_PARAMS),
                "--sizelimit",
                "0",
            ],
            log_dir / f"train_{dataset}_mscn.log",
            cwd=lecarb_root,
        )
        entry["mscn"] = _find_latest_model_name(model_root, mscn_prefix)
        if not entry["mscn"]:
            raise SystemExit(f"MSCN model not found under {model_root}")

    entry["base_version"] = base_version
    entry["train_workload"] = train_workload
    lock[dataset] = entry
    _save_model_lock(model_lock_path, lock)
    return lock


def _ensure_workloads(
    dataset: str,
    version: str,
    workload_names: List[str],
    seed: int,
    log_dir: Path,
    lecarb_root: Path,
) -> None:
    data_root = Path(os.environ.get("DATA_ROOT", "."))
    workload_root = data_root / dataset / "workload"
    missing = [
        name for name in workload_names if not (workload_root / f"{name}.pkl").exists()
    ]
    if not missing:
        return
    _log_step(log_dir, f"workload_gen: dataset={dataset} version={version} missing={','.join(missing)}")
    _run_cmd(
        [
            sys.executable,
            "-m",
            "lecarb",
            "workload",
            "gen",
            "-s",
            str(seed),
            "-d",
            dataset,
            "-v",
            version,
            "-w",
            "base",
            "--params",
            "{}",
        ],
        log_dir / f"workload_gen_{dataset}.log",
        cwd=lecarb_root,
    )


def _ensure_dataset_artifacts(
    dataset: str,
    version: str,
    log_dir: Path,
) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    data_root = Path(os.environ.get("DATA_ROOT", "."))
    csv_path = data_root / dataset / f"{version}.csv"
    pkl_path = csv_path.with_suffix(".pkl")
    table_path = data_root / dataset / f"{version}.table.pkl"

    if not csv_path.exists():
        raise SystemExit(f"Missing base CSV: {csv_path}")

    if not pkl_path.exists():
        _log_step(log_dir, f"csv2pkl: {csv_path}")
        df = pd.read_csv(csv_path)
        cat_cols = [k for k, d in df.dtypes.items() if d == "O"]
        if cat_cols:
            cat_type = pd.CategoricalDtype(ordered=True)
            df = df.astype({k: cat_type for k in cat_cols})
        df.to_pickle(pkl_path)
        (log_dir / f"csv2pkl_{dataset}_{version}.log").write_text(
            f"Converted {csv_path} -> {pkl_path}\n",
            encoding="utf-8",
        )

    if not table_path.exists():
        _log_step(log_dir, f"pkl2table: {dataset} {version}")
        _run_cmd(
            [
                sys.executable,
                "-m",
                "lecarb",
                "dataset",
                "table",
                "-d",
                dataset,
                "-v",
                version,
                "--overwrite",
            ],
            log_dir / f"pkl2table_{dataset}_{version}.log",
            cwd=repo_root / "existing_benchmarks" / "AreCELearnedYet",
        )


def _reload_base_table(
    dataset: str,
    version: str,
    log_dir: Path,
    repo_root: Path,
) -> None:
    data_root = Path(os.environ.get("DATA_ROOT", "."))
    csv_path = data_root / dataset / f"{version}.csv"
    if not csv_path.exists():
        raise SystemExit(f"Missing base CSV: {csv_path}")
    table_name = f"{dataset}_{version}"
    _log_step(log_dir, f"reload_base_table: {table_name} from {csv_path}")
    conn = _connect_pg()
    try:
        _load_csv_to_postgres(conn, csv_path, table_name)
    finally:
        conn.close()


def _version_from_update_file(path: Path, dataset: str) -> str:
    name = path.stem
    if name.startswith(f"{dataset}_"):
        name = name[len(dataset) + 1 :]
    if name.endswith("_updates"):
        name = name[: -len("_updates")]
    return name


def _run_workloads_for_version(
    version: str,
    workloads: List[str],
    dataset: str,
    base_version: str,
    estimators: List[str],
    seed: int,
    stat_target: int,
    log_dir: Path,
    lecarb_root: Path,
    model_lock: Optional[dict] = None,
) -> None:
    model_entry = (model_lock or {}).get(dataset, {})
    for workload in workloads:
        _log_step(log_dir, f"workload_label: {version} {workload}")
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
            log_dir / f"{version}_{workload}_label.log",
            cwd=lecarb_root,
        )

        for estimator in estimators:
            if estimator == "postgres":
                params = f"{{'version':'{base_version}','stat_target':{stat_target}}}"
            elif estimator == "naru":
                model_root = Path(os.environ.get("OUTPUT_ROOT", ".")) / "model" / dataset
                model_name = os.environ.get("NARU_MODEL") or model_entry.get("naru") or _find_latest_model_name(
                    model_root, f"{base_version}-"
                )
                if not model_name:
                    _log_step(log_dir, f"skip_estimator: {version} {workload} naru (no model)")
                    continue
                psample = int(os.environ.get("NARU_PSAMPLE", "2000"))
                params = f"{{'model':'{model_name}','psample':{psample}}}"
            elif estimator == "mscn":
                model_root = Path(os.environ.get("OUTPUT_ROOT", ".")) / "model" / dataset
                model_name = os.environ.get("MSCN_MODEL") or model_entry.get("mscn") or _find_latest_model_name(
                    model_root, f"{base_version}_{workload}-"
                )
                if not model_name:
                    _log_step(log_dir, f"skip_estimator: {version} {workload} mscn (no model)")
                    continue
                params = f"{{'model':'{model_name}'}}"
            else:
                _log_step(log_dir, f"skip_estimator: {version} {workload} {estimator}")
                continue

            _log_step(log_dir, f"workload_test: {version} {workload} {estimator}")
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
                    params,
                    "--overwrite",
                ],
                log_dir / f"{version}_{workload}_{estimator}.log",
                cwd=lecarb_root,
            )


def generate_skew_sql(args) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    log_dir = Path(args.log_dir)
    data_root = Path(args.data_root)
    plan_dir = Path(args.sql_plan_dir)

    _check_env(require_db=False, require_lecarb=False, require_base_csv=True)
    base_csv = Path(os.environ.get("BASE_CSV", repo_root / "data" / "census_original.csv"))
    data_dir = data_root / "census13"
    data_dir.mkdir(parents=True, exist_ok=True)
    _ensure_row_id_csv(base_csv)
    dst_base = data_dir / "original.csv"
    if not dst_base.exists():
        shutil.copy2(base_csv, dst_base)
    _ensure_row_id_csv(dst_base)

    _log_step(log_dir, f"generate_skew_sql: baselines -> {data_dir}")
    _run_cmd(
        [sys.executable, "-m", "driftbench.cli", "run-yaml", str(args.baseline_spec)],
        log_dir / "ce_prepare_data.log",
        cwd=repo_root,
    )

    skew_versions = _parse_csv_list(args.skew_versions)
    skew_values = _parse_csv_list(args.skew_values)
    minutes = len(skew_versions) if skew_versions else 0
    if minutes == 0:
        raise SystemExit("skew_versions is empty.")

    plan_dir.mkdir(parents=True, exist_ok=True)
    _log_step(log_dir, f"generate_skew_sql: plan_dir={plan_dir}")
    _run_cmd(
        [
            sys.executable,
            str(repo_root / "tasks" / "cardinality_estimation" / "ce_timeline_census.py"),
            "--cases",
            "skew",
            "--minutes",
            str(minutes),
            "--skew-versions",
            args.skew_versions,
            "--skew-values",
            args.skew_values,
            "--skew-columns",
            args.skew_columns,
            "--sql-update-columns",
            args.skew_update_columns,
            "--schema-path",
            str(args.schema_path),
            "--sql-plan-dir",
            str(plan_dir),
            "--sql-plan-format",
            "row_update",
            "--sql-table-mode",
            "inplace",
            "--sql-target-table",
            args.sql_target_table,
            "--sql-key-column",
            args.sql_key_column,
            "--db-apply-mode",
            "sql",
            "--sql-plan-only",
        ],
        log_dir / "ce_generate_sql_plan.log",
        cwd=repo_root,
    )


def apply_data_drift(args) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    log_dir = Path(args.log_dir)
    plan_dir = Path(args.sql_plan_dir)
    _check_env(require_db=True, require_lecarb=False, require_base_csv=False)

    files = _collect_update_files(args.files, plan_dir)
    if not files:
        raise SystemExit(f"No update files in {plan_dir}")

    for path in files:
        _log_step(log_dir, f"apply_data_drift: {path}")
        _run_cmd(
            [
                sys.executable,
                str(repo_root / "tasks" / "cardinality_estimation" / "apply_sql_updates.py"),
                "--plan-dir",
                str(path.parent),
                "--pattern",
                path.name,
                "--log-dir",
                str(log_dir),
            ],
            log_dir / f"apply_{path.stem}.log",
            cwd=repo_root,
        )


def apply_workload(args) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    lecarb_root = repo_root / "existing_benchmarks" / "AreCELearnedYet"
    log_dir = Path(args.log_dir)
    _check_env(require_db=True, require_lecarb=True, require_base_csv=False)

    workloads = _resolve_workload_names(args.workloads or _parse_csv_list(args.ce_workloads))
    if not workloads:
        raise SystemExit("No workloads provided.")

    # estimators = _parse_csv_list(args.ce_estimators) or ["postgres"]
    estimators = _parse_csv_list(args.ce_estimators) or ["naru", "mscn", "postgres"]
    estimators = ["mscn"]
    dataset = args.dataset
    base_version = args.base_version
    _ensure_workloads(
        dataset,
        base_version,
        workloads,
        args.seed,
        log_dir,
        lecarb_root,
    )

    _run_workloads_for_version(
        args.version,
        workloads,
        dataset,
        base_version,
        estimators,
        args.seed,
        args.stat_target,
        log_dir,
        lecarb_root,
    )


def apply_drift_and_workload(args) -> None:
    if args.workloads:
        args.workloads = _resolve_workload_names(args.workloads)
    apply_data_drift(args)
    apply_workload(args)


def apply_drift_and_workload_skew(args) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    lecarb_root = repo_root / "existing_benchmarks" / "AreCELearnedYet"
    log_dir = Path(args.log_dir)
    plan_dir = Path(args.sql_plan_dir)
    data_root = Path(os.environ.get("DATA_ROOT") or args.data_root)
    os.environ["DATA_ROOT"] = str(data_root)
    _check_env(require_db=True, require_lecarb=True, require_base_csv=False)

    datasets = [
        "census13",
    ]
    base_version = "original"
    skew_levels = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    workloads = [
        "census_original_uniform_sqls",
        "census_original_skew_sqls",
        "census_original_normal_sqls",
        "census_original_sqls_selectivity_1",
        "census_original_sqls_selectivity_2",
        "census_original_sqls_selectivity_3",
    ]
    estimators = ["postgres", "naru", "mscn"]
    model_lock_path = log_dir / "model_lock.json"
    model_lock = _load_model_lock(model_lock_path)
    if not model_lock:
        raise SystemExit(f"Model lock missing: {model_lock_path}")

    for dataset in datasets:
        base_table_name = f"{dataset}_{base_version}"
        _reload_base_table(dataset, base_version, log_dir, repo_root)
        _ensure_dataset_artifacts(dataset, base_version, log_dir)
        for level in skew_levels:
            _ensure_dataset_artifacts(
                dataset,
                f"{base_version}_skew_{level}",
                log_dir,
            )
        _run_workloads_for_version(
            base_version,
            workloads,
            dataset,
            base_version,
            estimators,
            args.seed,
            args.stat_target,
            log_dir,
            lecarb_root,
        )
        # NOTE: SQL apply path kept for reference; disabled in favor of CSV reload.
        # files = []
        # for level in skew_levels:
        #     path = plan_dir / f"{dataset}_{base_version}_skew_{level}_updates.csv"
        #     if not path.exists():
        #         raise SystemExit(f"Missing update file: {path}")
        #     files.append(path)
        #
        # for path in files:
        #     _log_step(log_dir, f"apply_data_drift: {path}")
        #     _run_cmd(
        #         [
        #             sys.executable,
        #             str(repo_root / "tasks" / "cardinality_estimation" / "apply_sql_updates.py"),
        #             "--plan-dir",
        #             str(path.parent),
        #             "--pattern",
        #             path.name,
        #             "--log-dir",
        #             str(log_dir),
        #         ],
        #         log_dir / f"apply_{path.stem}.log",
        #         cwd=repo_root,
        #     )
        #     version = _version_from_update_file(path, dataset)
        #     _run_workloads_for_version(
        #         version,
        #         workloads,
        #         dataset,
        #         base_version,
        #         estimators,
        #         args.seed,
        #         args.stat_target,
        #         log_dir,
        #         lecarb_root,
        #         model_lock=model_lock,
        #     )

        conn = _connect_pg()
        try:
            for level in skew_levels:
                version = f"{base_version}_skew_{level}"
                version_csv = data_root / dataset / f"{version}.csv"
                if not version_csv.exists():
                    raise SystemExit(f"Missing skew CSV: {version_csv}")
                _log_step(log_dir, f"reload_skew_csv: {version_csv} -> {base_table_name}")
                _load_csv_to_postgres(conn, version_csv, base_table_name)
                _run_workloads_for_version(
                    version,
                    workloads,
                    dataset,
                    base_version,
                    estimators,
                    args.seed,
                    args.stat_target,
                    log_dir,
                    lecarb_root,
                    model_lock=model_lock,
                )
        finally:
            conn.close()


def apply_drift_and_workload_cardinality(args) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    lecarb_root = repo_root / "existing_benchmarks" / "AreCELearnedYet"
    log_dir = Path(args.log_dir)
    plan_dir = Path(args.sql_plan_dir)
    data_root = Path(os.environ.get("DATA_ROOT") or args.data_root)
    os.environ["DATA_ROOT"] = str(data_root)
    _check_env(require_db=True, require_lecarb=True, require_base_csv=False)

    dataset = args.dataset
    base_version = args.base_version
    workloads = _resolve_workload_names(_parse_csv_list(args.ce_workloads))
    if not workloads:
        raise SystemExit("No workloads provided.")
    estimators = [e.lower() for e in _parse_csv_list(args.ce_estimators)]
    # if not estimators:
    # estimators = ["postgres", "naru", "mscn"]
    estimators = ["naru", "mscn"]
    estimators = ["postgres"]

    model_lock_path = log_dir / "model_lock.json"
    model_lock = _load_model_lock(model_lock_path)

    _ensure_workloads(dataset, base_version, workloads, args.seed, log_dir, lecarb_root)

    base_csv = data_root / dataset / f"{base_version}.csv"
    if not base_csv.exists():
        raise SystemExit(f"Missing base CSV: {base_csv}")
    schema = _load_schema(Path(args.schema_path), base_csv, base_version)

    from driftbench.core.data.single_table import SingleTableDriftGenerator

    gen = SingleTableDriftGenerator(str(base_csv), schema, base_version, seed=args.seed)
    base_df = gen.df.copy()
    base_rows = len(base_df)
    cardinality_df = base_df.copy()
    current_rows = base_rows

    base_table_name = f"{dataset}_{base_version}"
    sql_table_name = args.sql_target_table or base_table_name
    if sql_table_name != base_table_name:
        _log_step(
            log_dir,
            f"sql_target_table_override: {sql_table_name} -> {base_table_name}",
        )
        sql_table_name = base_table_name

    _reload_base_table(dataset, base_version, log_dir, repo_root)
    _ensure_dataset_artifacts(dataset, base_version, log_dir)
    _run_workloads_for_version(
        base_version,
        workloads,
        dataset,
        base_version,
        estimators,
        args.seed,
        args.stat_target,
        log_dir,
        lecarb_root,
        model_lock=model_lock,
    )

    temp_dir = plan_dir / "tmp"
    scales = _parse_float_list(args.cardinality_scales)
    if not scales:
        scales = [
            1 + args.cardinality_scale_step * i
            for i in range(1, args.cardinality_scale_steps + 1)
        ]

    conn = _connect_pg()
    try:
        for idx, scale in enumerate(scales, start=1):
            target_rows = int(round(base_rows * scale))
            if target_rows <= current_rows:
                df = cardinality_df.copy()
            else:
                source_df = base_df if args.cardinality_growth_mode == "base" else cardinality_df
                gen.df = source_df
                new_rows = gen._generate_rows_like_existing(target_rows - current_rows)
                df = pd.concat([cardinality_df, new_rows], ignore_index=True)
            cardinality_df = df

            version = f"{base_version}_cardinality_{idx}"
            version_csv = data_root / dataset / f"{version}.csv"
            version_csv.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(version_csv, index=False)
            _ensure_dataset_artifacts(dataset, version, log_dir)

            # NOTE: SQL insert path kept for reference; disabled in favor of CSV reload.
            # inserted = _append_rows_to_postgres(
            #     conn,
            #     sql_table_name,
            #     df,
            #     current_rows,
            #     temp_dir,
            # )
            # if inserted:
            #     current_rows = len(df)
            #     _log_step(
            #         log_dir,
            #         f"cardinality_insert: {version} scale={scale} rows={current_rows}",
            #     )
            # else:
            #     _log_step(
            #         log_dir,
            #         f"cardinality_insert_skip: {version} scale={scale} rows={len(df)}",
            #     )

            _log_step(log_dir, f"reload_cardinality_csv: {version_csv} -> {sql_table_name}")
            _load_csv_to_postgres(conn, version_csv, sql_table_name)
            current_rows = len(df)

            _run_workloads_for_version(
                version,
                workloads,
                dataset,
                base_version,
                estimators,
                args.seed,
                args.stat_target,
                log_dir,
                lecarb_root,
                model_lock=model_lock,
            )
    finally:
        conn.close()


def apply_baseline_workload(args) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    lecarb_root = repo_root / "existing_benchmarks" / "AreCELearnedYet"
    log_dir = Path(args.log_dir)
    _check_env(require_db=True, require_lecarb=True, require_base_csv=False)

    datasets = [
        "census13",
    ]
    base_version = "original"
    workloads = [
        "census_original_uniform_sqls",
        "census_original_skew_sqls",
        "census_original_normal_sqls",
        "census_original_sqls_selectivity_1",
        "census_original_sqls_selectivity_2",
        "census_original_sqls_selectivity_3",
    ]
    estimators = ["postgres", "naru", "mscn"]
    train_workload = "census_original_uniform_sqls"
    model_lock_path = log_dir / "model_lock.json"
    if "naru" in estimators or "mscn" in estimators:
        for dataset in datasets:
            _ensure_model_lock(
                dataset=dataset,
                base_version=base_version,
                train_workload=train_workload,
                estimators=estimators,
                seed=args.seed,
                log_dir=log_dir,
                lecarb_root=lecarb_root,
                model_lock_path=model_lock_path,
            )
    model_lock = _load_model_lock(model_lock_path)

    for dataset in datasets:
        _reload_base_table(dataset, base_version, log_dir, repo_root)
        _run_workloads_for_version(
            base_version,
            workloads,
            dataset,
            base_version,
            estimators,
            args.seed,
            args.stat_target,
            log_dir,
            lecarb_root,
            model_lock=model_lock,
        )


def train_models(args) -> None:
    repo_root = Path(os.environ["REPO_ROOT"])
    lecarb_root = repo_root / "existing_benchmarks" / "AreCELearnedYet"
    log_dir = Path(args.log_dir)
    _check_env(require_db=False, require_lecarb=True, require_base_csv=False)

    datasets = [
        "census13",
    ]
    base_version = "original"
    train_workload = "census_original_uniform_sqls"
    estimators = ["naru", "mscn"]
    model_lock_path = log_dir / "model_lock.json"

    for dataset in datasets:
        _ensure_model_lock(
            dataset=dataset,
            base_version=base_version,
            train_workload=train_workload,
            estimators=estimators,
            seed=args.seed,
            log_dir=log_dir,
            lecarb_root=lecarb_root,
            model_lock_path=model_lock_path,
        )


def run_task(args) -> None:
    if args.task != "skew_timeline":
        raise SystemExit(f"Unknown task: {args.task}")
    apply_workload(args)
    apply_data_drift(args)
    apply_workload(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="CE timeline runner (CLI only).")
    parser.add_argument("--env-file", default="")
    parser.add_argument("--log-dir", default="")
    parser.add_argument("--sql-plan-dir", default="")
    parser.add_argument("--data-root", default="")
    parser.add_argument("--base-csv", default="")
    parser.add_argument("--dataset", default="census13")
    parser.add_argument("--base-version", default="original")
    parser.add_argument("--sql-target-table", default="census13_original")
    parser.add_argument("--sql-key-column", default="row_id")
    parser.add_argument("--schema-path", default="")
    parser.add_argument("--baseline-spec", default="")
    parser.add_argument("--skew-versions", default="")
    parser.add_argument("--skew-values", default="")
    parser.add_argument("--skew-columns", default="age")
    parser.add_argument("--skew-update-columns", default="")
    parser.add_argument("--cardinality-scales", default="")
    parser.add_argument("--cardinality-scale-step", type=float, default=0.2)
    parser.add_argument("--cardinality-scale-steps", type=int, default=10)
    parser.add_argument(
        "--cardinality-growth-mode",
        choices=["base", "incremental"],
        default="base",
    )
    parser.add_argument("--ce-estimators", default="")
    parser.add_argument("--ce-workloads", default="")
    parser.add_argument("--stat-target", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=123)

    sub = parser.add_subparsers(dest="command")
    sub.add_parser("generate-skew-sql")

    apply_drift = sub.add_parser("apply-data-drift")
    apply_drift.add_argument("files", nargs="*")

    apply_wl = sub.add_parser("apply-workload")
    apply_wl.add_argument("version")
    apply_wl.add_argument("workloads", nargs="*")

    apply_both = sub.add_parser("apply-drift-and-workload")
    apply_both.add_argument("version")
    apply_both.add_argument("files", nargs="*")
    apply_both.add_argument("--workloads", default="")

    sub.add_parser("apply-drift-and-workload-skew")
    sub.add_parser("apply-drift-and-workload-cardinality")
    sub.add_parser("apply-baseline-workload")
    sub.add_parser("train-models")

    task = sub.add_parser("run-task")
    task.add_argument("task", default="skew_timeline")
    task.add_argument("version")
    task.add_argument("files", nargs="*")
    task.add_argument("--workloads", default="")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    os.environ["REPO_ROOT"] = str(repo_root)
    os.environ.setdefault("BASE_CSV", str(repo_root / "data" / "census_original.csv"))

    pythonpath = os.environ.get("PYTHONPATH", "")
    if str(repo_root) not in pythonpath.split(os.pathsep):
        os.environ["PYTHONPATH"] = f"{repo_root}{os.pathsep}{pythonpath}" if pythonpath else str(repo_root)

    env_file = Path(args.env_file) if args.env_file else repo_root / "tasks" / "cardinality_estimation" / ".env"
    _load_env(env_file)

    for key in ("OUTPUT_ROOT", "DATA_ROOT"):
        value = os.environ.get(key)
        if value:
            path = Path(value)
            if not path.is_absolute():
                os.environ[key] = str((repo_root / path).resolve())

    os.environ.setdefault(
        "WORKLOAD_CSV_ROOT",
        str(
            repo_root
            / "tasks"
            / "cardinality_estimation"
            / "output"
            / "workload"
            / "parametric"
        ),
    )

    if args.log_dir:
        log_dir = Path(args.log_dir)
    else:
        log_dir = Path(
            os.environ.get(
                "OUTPUT_ROOT",
                repo_root / "tasks" / "cardinality_estimation" / "ce_timeline_census",
            )
        ) / "log" / "ce_timeline"
    args.log_dir = str(log_dir)

    if args.sql_plan_dir:
        plan_dir = Path(args.sql_plan_dir)
    else:
        plan_dir = log_dir / "sql_plan"
    args.sql_plan_dir = str(plan_dir)

    if args.data_root:
        args.data_root = str(Path(args.data_root))
    else:
        args.data_root = str(repo_root / "tasks" / "cardinality_estimation" / "data")

    if args.base_csv:
        os.environ["BASE_CSV"] = args.base_csv

    if not args.schema_path:
        args.schema_path = str(
            repo_root
            / "tasks"
            / "cardinality_estimation"
            / "output"
            / "intermediate_yaml"
            / f"{args.dataset}_{args.base_version}_schema.json"
        )

    if not args.baseline_spec:
        args.baseline_spec = str(
            repo_root
            / "tasks"
            / "cardinality_estimation"
            / "specs"
            / "census_data_baselines.yaml"
        )

    if not args.skew_versions:
        args.skew_versions = os.environ.get(
            "SKEW_VERSIONS",
            "original_skew_2,original_skew_3,original_skew_4,original_skew_5,original_skew_6,original_skew_7,original_skew_8,original_skew_9,original_skew_10,original_skew_11",
        )
    if not args.skew_values:
        args.skew_values = os.environ.get(
            "SKEW_VALUES",
            "2,3,4,5,6,7,8,9,10,11",
        )
    if not args.skew_update_columns:
        args.skew_update_columns = os.environ.get("SKEW_UPDATE_COLUMNS", args.skew_columns)

    if not args.cardinality_scales:
        args.cardinality_scales = os.environ.get("CARDINALITY_SCALES", "")
    if abs(args.cardinality_scale_step - 0.2) < 1e-9:
        args.cardinality_scale_step = float(
            os.environ.get(
                "CARDINALITY_SCALE_STEP",
                os.environ.get("CARDINALITY_GROWTH_PER_MIN", "0.2"),
            )
        )
    if args.cardinality_scale_steps == 10:
        args.cardinality_scale_steps = int(
            os.environ.get(
                "CARDINALITY_SCALE_STEPS",
                os.environ.get("CARDINALITY_MINUTES", "10"),
            )
        )
    if args.cardinality_growth_mode == "base":
        args.cardinality_growth_mode = os.environ.get(
            "CARDINALITY_GROWTH_MODE",
            "base",
        )

    if not args.ce_estimators:
        args.ce_estimators = os.environ.get("CE_ESTIMATORS", "postgres")
    if not args.ce_workloads:
        args.ce_workloads = os.environ.get(
            "CE_WORKLOADS",
            "census_original_uniform_sqls,census_original_skew_sqls,census_original_normal_sqls,census_original_sqls_selectivity_1,census_original_sqls_selectivity_2,census_original_sqls_selectivity_3",
        )
    if args.stat_target == 10000:
        args.stat_target = int(os.environ.get("STAT_TARGET", "10000"))
    if args.seed == 123:
        args.seed = int(os.environ.get("SEED", "123"))

    if args.command == "generate-skew-sql":
        generate_skew_sql(args)
    elif args.command == "apply-data-drift":
        apply_data_drift(args)
    elif args.command == "apply-workload":
        apply_workload(args)
    elif args.command == "apply-drift-and-workload":
        if args.workloads:
            args.workloads = _parse_csv_list(args.workloads)
        apply_drift_and_workload(args)
    elif args.command == "apply-drift-and-workload-skew":
        apply_drift_and_workload_skew(args)
    elif args.command == "apply-drift-and-workload-cardinality":
        apply_drift_and_workload_cardinality(args)
    elif args.command == "apply-baseline-workload":
        apply_baseline_workload(args)
    elif args.command == "train-models":
        train_models(args)
    elif args.command == "run-task":
        if args.workloads:
            args.workloads = _parse_csv_list(args.workloads)
        run_task(args)
    else:
        parser.print_help()
        raise SystemExit(1)


if __name__ == "__main__":
    main()

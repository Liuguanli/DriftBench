"""Generate two SQL frequency samples; no database connection or execution."""

import argparse
from collections import Counter
import json
from pathlib import Path

from driftbench.api import QueryTemplate, apply_query_workload_mix_drift


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True,
                        help="New directory for generated SQL and mix.json")
    args = parser.parse_args()

    # Supply SQL that matches your dataset's table, columns and SQL dialect.
    templates = [
        QueryTemplate("lookup", "SELECT amount FROM orders WHERE order_id = 3;"),
        QueryTemplate("report", "SELECT region, SUM(amount) FROM orders GROUP BY region;"),
    ]
    result = apply_query_workload_mix_drift(
        templates,
        baseline_weights={"lookup": 0.8, "report": 0.2},
        target_weights={"lookup": 0.2, "report": 0.8},
        sample_size=100,
        seed=42,
    )
    # The API returns objects in memory. This teaching script writes the files.
    try:
        args.output_dir.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        parser.error("--output-dir already exists; choose a new directory")
    counts = {}
    for phase in ("baseline", "drifted"):
        sample = getattr(result, phase)
        sql = "\n".join(query.sql for query in sample) + "\n"
        (args.output_dir / f"{phase}.sql").write_text(sql, encoding="utf-8")
        counts[phase] = dict(Counter(query.template_id for query in sample))
    metadata = {
        "algorithm": result.algorithm,
        "semantic_hash": result.semantic_hash,
        "seed": result.seed,
        "sample_size": result.sample_size,
        "templates": {query.template_id: query.sql for query in templates},
        "baseline_weights": dict(result.baseline_weights),
        "target_weights": dict(result.target_weights),
        "observed_counts": counts,
    }
    (args.output_dir / "mix.json").write_text(
        json.dumps(metadata, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output_dir": str(args.output_dir), **metadata}, indent=2))


if __name__ == "__main__":
    main()

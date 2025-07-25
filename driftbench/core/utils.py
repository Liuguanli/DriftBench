import csv
import json
import os
from pathlib import Path
from typing import List, Dict
from datetime import datetime


def save_templates(templates: List[Dict], output_path: str):
    """
    Save templates to a specified JSON file path.
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        json.dump(templates, f, indent=2)


def save_sqls(sqls: List[str], output_path: str):
    """
    Save a list of SQL statements to a text file, one per line.

    Args:
        sqls (List[str]): List of SQL query strings.
        output_path (str): Path to the output file (.sql or .txt).
    """
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w") as f:
        for sql in sqls:
            f.write(sql.strip() + "\n")


def save_sqls_with_timestamps(sqls: List[str], timestamps: List[str], output_path: str) -> str:
    """
    Save a list of SQL queries and corresponding timestamps to a timestamped CSV file.
    
    Each row in the CSV will contain: [timestamp, sql]
    
    Returns the path to the saved CSV file.
    """
    assert len(sqls) == len(timestamps), "sqls and timestamps must have the same length"

    # Path(output_path).mkdir(parents=True, exist_ok=True)
    file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    # output_path = os.path.join(output_dir, f"queries_with_timestamps_{file_timestamp}.csv")

    with open(output_path, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "sql"])  # header
        for ts, sql in zip(timestamps, sqls):
            writer.writerow([ts, sql.strip()])

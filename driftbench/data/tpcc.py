"""TPC-C benchmark adapter.

TPC-C is the standard OLTP benchmark simulating an order-entry environment.
It exercises 9 tables through 5 transaction types at a scale defined by the
number of warehouses.

Quick start:
    from driftbench.data.tpcc import data as tpcc_data, queries as tpcc_queries

    tpcc_data(scale_factor=1).generate(output_dir="./artifacts")
    tpcc_queries().generate(output_dir="./artifacts")

Scale note:
    scale_factor == number of warehouses (W).
    Approximate row counts at W=1:
      warehouse: 1, district: 10, customer: 3 000, item: 10 000 (synth cap),
      stock: 10 000, orders: 3 000, order_line: ~30 000,
      new_order: ~900, history: 3 000.
    All tables except item grow linearly with W.
"""
from __future__ import annotations

import csv
import random
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Literal

from .base import BenchmarkArtifact, GenerationResult


# ---------------------------------------------------------------------------
# SQL transaction templates (5 TPC-C transaction types)
# ---------------------------------------------------------------------------

_TPCC_TRANSACTIONS: dict[str, str] = {
    "new_order": """\
-- TPC-C Transaction 1: New Order
-- Inserts a new customer order across warehouse, district, item, and stock.
BEGIN;

SELECT w_tax FROM warehouse WHERE w_id = :w_id;

SELECT d_tax, d_next_o_id
FROM district
WHERE d_w_id = :w_id AND d_id = :d_id
FOR UPDATE;

UPDATE district
SET d_next_o_id = d_next_o_id + 1
WHERE d_w_id = :w_id AND d_id = :d_id;

SELECT c_discount, c_last, c_credit
FROM customer
WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id;

INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
VALUES (:o_id, :d_id, :w_id, :c_id, NOW(), NULL, :ol_cnt, :all_local);

INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
VALUES (:o_id, :d_id, :w_id);

-- For each order line:
SELECT i_price, i_name, i_data FROM item WHERE i_id = :ol_i_id;
SELECT s_quantity, s_data, s_dist_01 FROM stock WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id FOR UPDATE;
UPDATE stock SET s_quantity = :new_qty WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
VALUES (:o_id, :d_id, :w_id, :ol_number, :ol_i_id, :ol_supply_w_id, NULL, :ol_quantity, :ol_amount, :ol_dist_info);

COMMIT;
""",
    "payment": """\
-- TPC-C Transaction 2: Payment
-- Processes a customer payment, updating warehouse and district YTD amounts.
BEGIN;

UPDATE warehouse SET w_ytd = w_ytd + :h_amount WHERE w_id = :w_id;
SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = :w_id;

UPDATE district SET d_ytd = d_ytd + :h_amount WHERE d_w_id = :w_id AND d_id = :d_id;
SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = :w_id AND d_id = :d_id;

-- Customer lookup (by id or by last name):
SELECT c_id, c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip,
       c_phone, c_since, c_credit, c_credit_lim, c_discount, c_balance, c_data
FROM customer
WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id
FOR UPDATE;

UPDATE customer
SET c_balance = c_balance - :h_amount,
    c_ytd_payment = c_ytd_payment + :h_amount,
    c_payment_cnt = c_payment_cnt + 1
WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;

INSERT INTO history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
VALUES (:c_id, :c_d_id, :c_w_id, :d_id, :w_id, NOW(), :h_amount, :h_data);

COMMIT;
""",
    "order_status": """\
-- TPC-C Transaction 3: Order Status
-- Reads the most recent order and its lines for a given customer.
BEGIN;

-- Customer lookup:
SELECT c_id, c_first, c_middle, c_last, c_balance
FROM customer
WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :c_id;

-- Most recent order:
SELECT o_id, o_carrier_id, o_entry_d, o_ol_cnt
FROM orders
WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_c_id = :c_id
ORDER BY o_id DESC
LIMIT 1;

-- Order lines:
SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d
FROM order_line
WHERE ol_w_id = :w_id AND ol_d_id = :d_id AND ol_o_id = :o_id;

COMMIT;
""",
    "delivery": """\
-- TPC-C Transaction 4: Delivery
-- Delivers the oldest undelivered order in each district of a warehouse.
BEGIN;

-- For each district d_id in 1..10:
SELECT no_o_id
FROM new_order
WHERE no_w_id = :w_id AND no_d_id = :d_id
ORDER BY no_o_id ASC
LIMIT 1
FOR UPDATE;

DELETE FROM new_order WHERE no_w_id = :w_id AND no_d_id = :d_id AND no_o_id = :no_o_id;

SELECT o_c_id FROM orders WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_id = :no_o_id;

UPDATE orders
SET o_carrier_id = :o_carrier_id
WHERE o_w_id = :w_id AND o_d_id = :d_id AND o_id = :no_o_id;

UPDATE order_line
SET ol_delivery_d = NOW()
WHERE ol_w_id = :w_id AND ol_d_id = :d_id AND ol_o_id = :no_o_id;

SELECT SUM(ol_amount) AS total
FROM order_line
WHERE ol_w_id = :w_id AND ol_d_id = :d_id AND ol_o_id = :no_o_id;

UPDATE customer
SET c_balance = c_balance + :total,
    c_delivery_cnt = c_delivery_cnt + 1
WHERE c_w_id = :w_id AND c_d_id = :d_id AND c_id = :o_c_id;

COMMIT;
""",
    "stock_level": """\
-- TPC-C Transaction 5: Stock Level
-- Counts items in the last 20 orders of a district that are below a stock threshold.
BEGIN;

SELECT d_next_o_id FROM district WHERE d_w_id = :w_id AND d_id = :d_id;

SELECT COUNT(DISTINCT ol_i_id) AS low_stock
FROM order_line
JOIN stock ON ol_i_id = s_i_id AND s_w_id = :w_id
WHERE ol_w_id = :w_id
  AND ol_d_id = :d_id
  AND ol_o_id BETWEEN :d_next_o_id - 20 AND :d_next_o_id - 1
  AND s_quantity < :threshold;

COMMIT;
""",
}

_TPCC_DDL = """\
-- TPC-C Schema (PostgreSQL-compatible)

CREATE TABLE IF NOT EXISTS warehouse (
    w_id        INTEGER PRIMARY KEY,
    w_name      VARCHAR(10),
    w_street_1  VARCHAR(20),
    w_street_2  VARCHAR(20),
    w_city      VARCHAR(20),
    w_state     CHAR(2),
    w_zip       CHAR(9),
    w_tax       NUMERIC(4,4),
    w_ytd       NUMERIC(12,2)
);

CREATE TABLE IF NOT EXISTS district (
    d_id        INTEGER,
    d_w_id      INTEGER REFERENCES warehouse(w_id),
    d_name      VARCHAR(10),
    d_street_1  VARCHAR(20),
    d_street_2  VARCHAR(20),
    d_city      VARCHAR(20),
    d_state     CHAR(2),
    d_zip       CHAR(9),
    d_tax       NUMERIC(4,4),
    d_ytd       NUMERIC(12,2),
    d_next_o_id INTEGER,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE IF NOT EXISTS customer (
    c_id          INTEGER,
    c_d_id        INTEGER,
    c_w_id        INTEGER,
    c_first       VARCHAR(16),
    c_middle      CHAR(2),
    c_last        VARCHAR(16),
    c_street_1    VARCHAR(20),
    c_street_2    VARCHAR(20),
    c_city        VARCHAR(20),
    c_state       CHAR(2),
    c_zip         CHAR(9),
    c_phone       CHAR(16),
    c_since       TIMESTAMP,
    c_credit      CHAR(2),
    c_credit_lim  NUMERIC(12,2),
    c_discount    NUMERIC(4,4),
    c_balance     NUMERIC(12,2),
    c_ytd_payment NUMERIC(12,2),
    c_payment_cnt SMALLINT,
    c_delivery_cnt SMALLINT,
    c_data        VARCHAR(500),
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE IF NOT EXISTS history (
    h_c_id    INTEGER,
    h_c_d_id  INTEGER,
    h_c_w_id  INTEGER,
    h_d_id    INTEGER,
    h_w_id    INTEGER,
    h_date    TIMESTAMP,
    h_amount  NUMERIC(6,2),
    h_data    VARCHAR(24)
);

CREATE TABLE IF NOT EXISTS item (
    i_id    INTEGER PRIMARY KEY,
    i_im_id INTEGER,
    i_name  VARCHAR(24),
    i_price NUMERIC(5,2),
    i_data  VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS stock (
    s_i_id       INTEGER,
    s_w_id       INTEGER,
    s_quantity   SMALLINT,
    s_dist_01    CHAR(24),
    s_dist_02    CHAR(24),
    s_dist_03    CHAR(24),
    s_dist_04    CHAR(24),
    s_dist_05    CHAR(24),
    s_dist_06    CHAR(24),
    s_dist_07    CHAR(24),
    s_dist_08    CHAR(24),
    s_dist_09    CHAR(24),
    s_dist_10    CHAR(24),
    s_ytd        NUMERIC(8,2),
    s_order_cnt  SMALLINT,
    s_remote_cnt SMALLINT,
    s_data       VARCHAR(50),
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE IF NOT EXISTS orders (
    o_id         INTEGER,
    o_d_id       INTEGER,
    o_w_id       INTEGER,
    o_c_id       INTEGER,
    o_entry_d    TIMESTAMP,
    o_carrier_id INTEGER,
    o_ol_cnt     SMALLINT,
    o_all_local  SMALLINT,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE IF NOT EXISTS new_order (
    no_o_id INTEGER,
    no_d_id INTEGER,
    no_w_id INTEGER,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE IF NOT EXISTS order_line (
    ol_o_id        INTEGER,
    ol_d_id        INTEGER,
    ol_w_id        INTEGER,
    ol_number      INTEGER,
    ol_i_id        INTEGER,
    ol_supply_w_id INTEGER,
    ol_delivery_d  TIMESTAMP,
    ol_quantity    SMALLINT,
    ol_amount      NUMERIC(6,2),
    ol_dist_info   CHAR(24),
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);
"""


@dataclass
class TPCCData(BenchmarkArtifact):
    """Generate synthetic TPC-C data artifacts.

    scale_factor is treated as the number of warehouses (W).
    All tables except `item` grow linearly with W.

    Args:
        scale_factor: Number of warehouses. Default 1.
        mode: "synth" generates CSV files locally. "plan" writes a shell
              script for server-side generation via BenchBase or similar.
    """

    scale_factor: int | float = 1
    mode: Literal["synth", "plan"] = "synth"

    benchmark: str = "tpcc"
    artifact_type: str = "data"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpcc" / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpcc_data_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-C data (W={self._w()}) already exists at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-C data (W={self._w()}) → {out_dir}")

        ddl = self._write_text(out_dir / "tpcc_schema.sql", _TPCC_DDL)

        if self.mode == "plan":
            script = self._write_text(out_dir / "generate_tpcc_data.sh", self._plan_script())
            script.chmod(0o755)
            metadata = self._write_json(
                out_dir / "tpcc_data_manifest.json",
                {
                    "benchmark": self.benchmark,
                    "artifact_type": self.artifact_type,
                    "scale_factor": self._w(),
                    "mode": self.mode,
                    "files": self._paths_relative_to(root, [ddl, script]),
                    "note": (
                        "Plan mode: run generate_tpcc_data.sh on a host with BenchBase "
                        "or a compatible TPC-C loader."
                    ),
                },
            )
            return self._result(root, [ddl, script], metadata)

        files = self._generate_synth(out_dir)
        files.insert(0, ddl)
        metadata = self._write_json(
            out_dir / "tpcc_data_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "scale_factor": self._w(),
                "mode": self.mode,
                "tables": {
                    "warehouse": self._w(),
                    "district": 10 * self._w(),
                    "customer": 3000 * self._w(),
                    "item": min(100000, 10000 * self._w()),
                    "stock": min(100000, 10000 * self._w()),
                    "orders": 3000 * self._w(),
                    "new_order": 900 * self._w(),
                    "order_line": 30000 * self._w(),
                    "history": 3000 * self._w(),
                },
                "files": self._paths_relative_to(root, files),
                "note": (
                    "Synthetic TPC-C data for onboarding and API testing. "
                    "Not a standards-compliant TPC-C dataset."
                ),
            },
        )
        return self._result(root, files, metadata)

    def _w(self) -> int:
        return max(1, int(round(float(self.scale_factor))))

    def _generate_synth(self, out_dir: Path) -> list[Path]:
        w = self._w()
        rng = random.Random(42)
        start = date(2020, 1, 1)
        files: list[Path] = []

        # warehouse
        files.append(self._write_csv(out_dir / "warehouse.csv",
            ["w_id", "w_name", "w_street_1", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"],
            [[i, f"WH{i}", f"Street{i}", f"City{i}", "CA", "12345-6789",
              f"{rng.uniform(0.1, 0.2):.4f}", "300000.00"]
             for i in range(1, w + 1)]))

        # district (10 per warehouse)
        files.append(self._write_csv(out_dir / "district.csv",
            ["d_id", "d_w_id", "d_name", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"],
            [[d, wid, f"Dist{d}W{wid}", f"City{wid}", "CA", "12345-0000",
              f"{rng.uniform(0.1, 0.2):.4f}", "30000.00", 3001]
             for wid in range(1, w + 1) for d in range(1, 11)]))

        # item (capped at 10 000 * w, max 100 000)
        item_count = min(100000, 10000 * w)
        files.append(self._write_csv(out_dir / "item.csv",
            ["i_id", "i_name", "i_price", "i_data"],
            [[i, f"Item{i:09d}", f"{rng.uniform(1, 100):.2f}", f"item_data_{i}"]
             for i in range(1, item_count + 1)]))

        # customer (3000 per warehouse per district)
        cust_rows: list[list] = []
        for wid in range(1, w + 1):
            for did in range(1, 11):
                for cid in range(1, 301):  # 300 per district = 3000 per warehouse
                    cust_rows.append([
                        cid, did, wid, f"First{cid}", "OE", f"Last{cid % 1000:03d}",
                        f"Street{cid}", f"City{wid}", "CA", "12345-0000",
                        f"555-{cid:07d}", "GC", "50000.00",
                        f"{rng.uniform(0, 0.5):.4f}", "-10.00", "10.00",
                        1, 0, f"cust_data_{cid}",
                    ])
        files.append(self._write_csv(out_dir / "customer.csv",
            ["c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last",
             "c_street_1", "c_city", "c_state", "c_zip", "c_phone",
             "c_credit", "c_credit_lim", "c_discount", "c_balance",
             "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"],
            cust_rows))

        # stock (one row per item per warehouse, capped)
        stock_count = min(100000, 10000 * w)
        stock_rows: list[list] = []
        for wid in range(1, w + 1):
            for iid in range(1, stock_count // w + 1):
                stock_rows.append([iid, wid, rng.randint(10, 100),
                                   f"dist_{iid:04d}", 0, 0, 0, f"stock_data_{iid}"])
        files.append(self._write_csv(out_dir / "stock.csv",
            ["s_i_id", "s_w_id", "s_quantity", "s_dist_01", "s_ytd",
             "s_order_cnt", "s_remote_cnt", "s_data"],
            stock_rows))

        # orders + new_order + order_line
        orders_rows: list[list] = []
        new_order_rows: list[list] = []
        ol_rows: list[list] = []
        history_rows: list[list] = []

        for wid in range(1, w + 1):
            for did in range(1, 11):
                for oid in range(1, 301):  # 300 orders per district
                    ol_cnt = rng.randint(5, 15)
                    orders_rows.append([oid, did, wid, rng.randint(1, 300),
                                        (start + timedelta(days=oid % 365)).isoformat(),
                                        None if oid > 270 else rng.randint(1, 10),
                                        ol_cnt, 1])
                    if oid > 270:  # last 30 are new orders
                        new_order_rows.append([oid, did, wid])
                    for lno in range(1, ol_cnt + 1):
                        ol_rows.append([oid, did, wid, lno, rng.randint(1, item_count),
                                        wid, None if oid > 270 else (start + timedelta(days=(oid + lno) % 365)).isoformat(),
                                        rng.randint(1, 10), f"{rng.uniform(0.01, 99.99):.2f}",
                                        f"dist_{lno:04d}"])

                    history_rows.append([rng.randint(1, 300), did, wid, did, wid,
                                         (start + timedelta(days=oid % 365)).isoformat(),
                                         f"{rng.uniform(1, 500):.2f}", f"WH{wid}    Dist{did}"])

        files.append(self._write_csv(out_dir / "orders.csv",
            ["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"],
            orders_rows))
        files.append(self._write_csv(out_dir / "new_order.csv",
            ["no_o_id", "no_d_id", "no_w_id"], new_order_rows))
        files.append(self._write_csv(out_dir / "order_line.csv",
            ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id",
             "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"],
            ol_rows))
        files.append(self._write_csv(out_dir / "history.csv",
            ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id",
             "h_date", "h_amount", "h_data"],
            history_rows))

        return files

    def _write_csv(self, path: Path, header: list[str], rows: list[list]) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        return path

    def _plan_script(self) -> str:
        return (
            "#!/usr/bin/env bash\n"
            "set -euo pipefail\n\n"
            f"WAREHOUSES={self._w()}\n"
            "# BenchBase TPC-C loader example:\n"
            "# java -jar benchbase.jar -b tpcc -c config/tpcc_config.xml \\\n"
            "#   --create=true --load=true --execute=false\n"
            "# Set scalefactor=${WAREHOUSES} in config/tpcc_config.xml\n\n"
            f'echo "Load TPC-C with ${{WAREHOUSES}} warehouse(s)"\n'
        )


@dataclass
class TPCCQueries(BenchmarkArtifact):
    """Generate TPC-C SQL transaction templates (all 5 transaction types).

    Each transaction type is written as a separate .sql file with
    named placeholders (:param_name) for runtime binding.
    """

    benchmark: str = "tpcc"
    artifact_type: str = "queries"

    def generate(self, output_dir: str | Path | None = None, force: bool = False) -> GenerationResult:
        root = self._require_output_dir(output_dir)
        out_dir = root / "tpcc" / "queries"
        out_dir.mkdir(parents=True, exist_ok=True)

        if not force:
            existing = self._load_existing(out_dir / "tpcc_queries_manifest.json", root)
            if existing is not None:
                print(f"[driftbench] TPC-C queries already exist at {out_dir}. Reusing.")
                return existing
        print(f"[driftbench] Generating TPC-C queries → {out_dir}")

        files: list[Path] = []
        for name, sql in _TPCC_TRANSACTIONS.items():
            files.append(self._write_text(out_dir / f"{name}.sql", sql))

        # Combined workload bundle
        bundle = "\n".join(
            f"-- === {name.upper()} ===\n{sql}"
            for name, sql in _TPCC_TRANSACTIONS.items()
        )
        files.append(self._write_text(out_dir / "tpcc_all_transactions.sql", bundle))

        metadata = self._write_json(
            out_dir / "tpcc_queries_manifest.json",
            {
                "benchmark": self.benchmark,
                "artifact_type": self.artifact_type,
                "transaction_types": list(_TPCC_TRANSACTIONS.keys()),
                "query_count": len(_TPCC_TRANSACTIONS),
                "files": self._paths_relative_to(root, files),
                "note": (
                    "TPC-C transactions use named placeholders (:param). "
                    "Bind values at runtime with your preferred DB driver."
                ),
            },
        )
        return self._result(root, files, metadata)


def data(scale_factor: int | float = 1, mode: Literal["synth", "plan"] = "synth") -> TPCCData:
    """Return a TPCCData adapter.

    Args:
        scale_factor: Number of warehouses (W). Scales all tables except item.
        mode: "synth" generates CSV files locally (default).
              "plan" writes a shell script for BenchBase-based generation.
    """
    return TPCCData(scale_factor=scale_factor, mode=mode)


def queries() -> TPCCQueries:
    """Return a TPCCQueries adapter producing all 5 TPC-C transaction templates."""
    return TPCCQueries()

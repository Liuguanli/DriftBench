import unittest
from pathlib import Path

from driftbench.data.tpcds import TPCDSData
from ..helpers import BenchmarkAdapterTestMixin


class TPCDSAdapterTests(BenchmarkAdapterTestMixin, unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()
        self._tmpdir = self._default_data_tmpdir

    def test_as_csv_writes_tpcds_header_and_preserves_rows(self) -> None:
        import pandas as pd

        out = Path(self._tmpdir) / "out"
        result = TPCDSData(scale_factor=1).generate(output_dir=out)
        csv_result = result.as_csv()

        store_sales = next(f for f in csv_result.files if f.name == "store_sales.csv")
        df = pd.read_csv(store_sales)
        self.assertEqual(
            list(df.columns),
            [
                "ss_sold_date_sk", "ss_item_sk", "ss_customer_sk", "ss_store_sk",
                "ss_quantity", "ss_net_paid", "ss_net_profit",
            ],
        )
        # store_sales = 10000 rows per SF; header must not inflate/consume rows.
        self.assertEqual(len(df), 10000)

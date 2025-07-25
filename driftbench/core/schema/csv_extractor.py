import pandas as pd
import numpy as np
from pathlib import Path
from .base import BaseSchemaExtractor

class CSVSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, csv_path: str, sample_size: int = 0, categorical_threshold: int = 10):
        self.csv_path = csv_path
        self.sample_size = sample_size
        self.categorical_threshold = categorical_threshold

    def infer_logical_type(self, series, categorical_threshold=10):
        import pandas.api.types as ptypes

        if ptypes.is_numeric_dtype(series):
            return "numeric"
        elif ptypes.is_bool_dtype(series):
            return "boolean"
        elif ptypes.is_datetime64_any_dtype(series):
            return "datetime"
        elif ptypes.is_string_dtype(series):
            sample = series.dropna().head(5)
            try:
                pd.to_datetime(sample, format="%Y-%m-%d %H:%M:%S.%f", errors="raise")
                return "datetime"
            except Exception:
                return "categorical" if series.nunique() < categorical_threshold else "string"
        return "categorical" if series.nunique() < categorical_threshold else "string"

    def extract_schema(self):
        df = pd.read_csv(self.csv_path, nrows=self.sample_size) if self.sample_size > 0 else pd.read_csv(self.csv_path)
        schema = {}

        for col in df.columns:
            col_data = df[col].dropna()
            logical_type = self.infer_logical_type(col_data, self.categorical_threshold)
            sample_values = col_data.unique().tolist()[:self.categorical_threshold] if logical_type != "categorical" else col_data.unique().tolist()

            col_info = {
                "num_unique": col_data.nunique(),
                "sample_values": sample_values,
                "logical_type": logical_type
            }

            if np.issubdtype(col_data.dtype, np.number) or np.issubdtype(col_data.dtype, np.datetime64):
                try:
                    col_info["range"] = {
                        "min": col_data.min(),
                        "max": col_data.max()
                    }
                    col_info["cdf"] = {
                        f"{int(q*100)}%": col_data.quantile(q)
                        for q in [0.0, 0.25, 0.5, 0.75, 1.0]
                    }
                except Exception:
                    col_info["range"] = {}
                    col_info["cdf"] = {}

            schema[col] = col_info

        filename = Path(self.csv_path).stem
        return {
            "source": "csv",
            "tables": {
                filename: {
                    "columns": schema,
                    "num_rows": len(df)
                }
            }
        }

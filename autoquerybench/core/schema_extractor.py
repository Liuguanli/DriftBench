from abc import ABC, abstractmethod
import pandas as pd
import psycopg2
import psycopg2.extras
import numpy as np

import pandas as pd
import json
from pathlib import Path
from collections import defaultdict
from psycopg2.extras import RealDictCursor


class BaseSchemaExtractor(ABC):
    @abstractmethod
    def extract_schema(self):
        """Extract table schema and return a dict with column names, types, and stats."""
        pass


class CSVSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, csv_path: str, sample_size: int = 0, categorical_threshold: int = 10):
        self.csv_path = csv_path
        self.sample_size = sample_size
        self.categorical_threshold = categorical_threshold

    def infer_logical_type(self, pandas_series, categorical_threshold=10):
        """Infer logical (semantic) type from pandas Series."""
        import pandas.api.types as ptypes

        if ptypes.is_numeric_dtype(pandas_series):
            return "numeric"
        elif ptypes.is_bool_dtype(pandas_series):
            return "boolean"
        elif ptypes.is_datetime64_any_dtype(pandas_series):
            return "datetime"
        elif ptypes.is_string_dtype(pandas_series):
                # try parsing first 5 non-null values
            sample = pandas_series.dropna().head(5)
            try:
                parsed = pd.to_datetime(sample, errors="raise")
                return "datetime"
            except Exception:
                return "categorical" if pandas_series.nunique() < categorical_threshold else "string"
        else:
            return "categorical" if pandas_series.nunique() < categorical_threshold else "string"


    def extract_schema(self):
        if self.sample_size > 0:
            df = pd.read_csv(self.csv_path, nrows=self.sample_size)
        else:
            df = pd.read_csv(self.csv_path)
        schema = {}

        for col in df.columns:
            col_data = df[col].dropna()
            # dtype = str(col_data.dtype)
            logical_type = self.infer_logical_type(col_data, self.categorical_threshold)
            if logical_type == "categorical":
                sample_values = col_data.unique().tolist()
            else:
                sample_values = col_data.unique()[:self.categorical_threshold].tolist()

            col_info = {
                # "type": dtype,
                "num_unique": col_data.nunique(),
                # "sample_values": col_data.unique()[:10].tolist()
                "sample_values": sample_values
            }
            col_info["logical_type"] = logical_type

            if np.issubdtype(col_data.dtype, np.number) or np.issubdtype(col_data.dtype, np.datetime64):
                try:
                    col_info["range"] = {
                        "min": col_data.min(),
                        "max": col_data.max()
                    }

                    # CDF approximation using quantiles
                    quantiles = [0.0, 0.25, 0.5, 0.75, 1.0]
                    col_info["cdf"] = {
                        f"{int(q*100)}%": col_data.quantile(q)
                        for q in quantiles
                    }
                except Exception as e:
                    col_info["range"] = {}
                    col_info["cdf"] = {}

            schema[col] = col_info

            filename = Path(self.csv_path).stem

        return {
                    "source": "csv",
                    "tables": {
                        f"{filename}": {
                            "columns": schema,
                            "num_rows": len(df)
                        }
                    }
                }

class PostgresSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, db_config: dict, schema_name: str, sample_size: int):
        self.db_config = db_config
        self.schema_name = schema_name
        self.sample_size = sample_size

    def get_column_stats(self, cursor, table, column):
        cursor.execute(f"""
            SELECT 
                COUNT(DISTINCT {column}) AS num_unique,
                MIN({column}) AS min_val,
                MAX({column}) AS max_val,
                PERCENTILE_CONT(ARRAY[0.0, 0.25, 0.5, 0.75, 1.0]) WITHIN GROUP (ORDER BY {column}) AS percentiles
            FROM "{self.schema_name}"."{table}"
            WHERE {column} IS NOT NULL
        """)
        row = cursor.fetchone()
        return row
    
    def infer_logical_type(self, dtype_or_series):
        categorical_threshold = 20

        # case 1: passed in a pandas Series
        if hasattr(dtype_or_series, "nunique"):
            return "categorical" if dtype_or_series.nunique() < categorical_threshold else "string"
        
        # case 2: passed in a string data type
        dtype = dtype_or_series.lower()
        if dtype in ['integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision']:
            return "numeric"
        elif dtype in ['boolean']:
            return "numeric"
        elif dtype in ['timestamp without time zone', 'timestamp with time zone', 'date']:
            return "datetime"
        elif dtype in ['character varying', 'text', 'character']:
            return "string"
        else:
            return "categorical"

    
    def _get_column_stats(self, cursor, table, column):
        cursor.execute(f"""
            SELECT 
                COUNT(DISTINCT {column}) AS num_unique,
                MIN({column}) AS min_val,
                MAX({column}) AS max_val,
                PERCENTILE_CONT(ARRAY[0.0, 0.25, 0.5, 0.75, 1.0]) WITHIN GROUP (ORDER BY {column}) AS percentiles
            FROM "{self.schema_name}"."{table}"
            WHERE {column} IS NOT NULL
        """)
        row = cursor.fetchone()
        return row
    
    def _get_numeric_stats(self, cursor, table, column):
        cursor.execute(f"""
            SELECT 
                COUNT(DISTINCT {column}) AS num_unique,
                MIN({column}) AS min_val,
                MAX({column}) AS max_val,
                PERCENTILE_CONT(ARRAY[0.0, 0.25, 0.5, 0.75, 1.0]) 
                    WITHIN GROUP (ORDER BY {column}) AS percentiles
            FROM "{self.schema_name}"."{table}"
            WHERE {column} IS NOT NULL
        """)
        return cursor.fetchone()
    
    def _get_sample_values(self, cursor, table, column, all_values=False):
        limit = "" if all_values else "LIMIT 10"
        cursor.execute(f"""
            SELECT DISTINCT {column}
            FROM "{self.schema_name}"."{table}"
            WHERE {column} IS NOT NULL
            {limit}
        """)
        return [row[column] for row in cursor.fetchall()]
    
    def extract_schema(self):
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        output = {"tables":{}}

        cursor.execute(f"""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema = '{self.schema_name}'
        """)
        tables = cursor.fetchall()

        for table_info in tables:
            # print("table_info", table_info)
        #     schema = table_info['table_schema']
            table_name = table_info['table_name']
            cursor.execute(f'SELECT COUNT(*) FROM "{self.schema_name}"."{table_name}"')
            num_rows = cursor.fetchone()['count']

            print("table", table_name, "num_rows", num_rows)
            
            cursor.execute(f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            """, (self.schema_name, table_name))
            columns = cursor.fetchall()

            table_result = {
                # "source": f"{self.schema_name}.{table_name}",
                "num_rows": num_rows,
                "columns": {}
            }

            for col in columns:
                col_name = col['column_name']
                col_type = col['data_type']
                logical_type = self.infer_logical_type(col_type)

                if logical_type in ["numeric"]:
                    stats = self._get_numeric_stats(cursor, table_name, col_name)
                    samples = self._get_sample_values(cursor, table_name, col_name)
                    col_result = {
                        "num_unique": stats["num_unique"],
                        "sample_values": samples,
                        "logical_type": logical_type,
                        "range": {
                            "min": stats["min_val"],
                            "max": stats["max_val"]
                        }
                    }
                    if stats["percentiles"] is not None:
                        col_result["cdf"] = {
                            "0%": stats["percentiles"][0],
                            "25%": stats["percentiles"][1],
                            "50%": stats["percentiles"][2],
                            "75%": stats["percentiles"][3],
                            "100%": stats["percentiles"][4],
                        }   
                else:
                    samples = self._get_sample_values(cursor, table_name, col_name, all_values=False)
                    col_result = {
                        "num_unique": len(samples),
                        "sample_values": samples,
                        "logical_type": logical_type
                    }
                    if logical_type == "datetime":
                        cursor.execute(f"""
                            SELECT MIN({col_name}) AS min_val, MAX({col_name}) AS max_val
                            FROM "{table_name}"
                            WHERE {col_name} IS NOT NULL
                        """)
                        result = cursor.fetchone()
                        col_result["range"] = {
                            "min": result["min_val"],
                            "max": result["max_val"]
                        }
                # col_result["column"] = f"{self.schema_name}.{table_name}.{col_name}"
                # print(col_result)
                table_result["columns"][col_name] = col_result

            output["tables"][f"{self.schema_name}.{table_name}"] = table_result
        output["source"] = "database"
        return output

    # def extract_schema(self):
    #     conn = psycopg2.connect(**self.db_config)
    #     cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    #     # Column types
    #     cur.execute("""
    #         SELECT column_name, data_type
    #         FROM information_schema.columns
    #         WHERE table_name = %s
    #     """, (self.table_name,))
    #     columns = {row["column_name"]: {"type": row["data_type"]} for row in cur.fetchall()}

    #     # Stats from pg_stats
    #     cur.execute("""
    #         SELECT attname, null_frac, n_distinct, most_common_vals, histogram_bounds
    #         FROM pg_stats
    #         WHERE tablename = %s
    #     """, (self.table_name,))
    #     for row in cur.fetchall():
    #         col = row["attname"]
    #         if col in columns:
    #             columns[col].update({
    #                 "null_frac": row["null_frac"],
    #                 "n_distinct": row["n_distinct"],
    #                 "most_common_vals": row["most_common_vals"],
    #                 "histogram_bounds": row["histogram_bounds"]
    #             })

    #     cur.close()
    #     conn.close()

    #     return {
    #         "source": "postgres",
    #         "table": self.table_name,
    #         "columns": columns
    #     }

def get_schema_extractor(source_type: str, **kwargs) -> BaseSchemaExtractor:
    """Factory function for schema extractors."""
    if source_type == "csv":
        return CSVSchemaExtractor(**kwargs)
    elif source_type == "postgres":
        return PostgresSchemaExtractor(**kwargs)
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")
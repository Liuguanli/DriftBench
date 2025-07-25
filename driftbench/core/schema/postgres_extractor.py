import psycopg2
import psycopg2.extras
from .base import BaseSchemaExtractor

class PostgresSchemaExtractor(BaseSchemaExtractor):
    def __init__(self, db_config: dict, schema_name: str, sample_size: int):
        self.db_config = db_config
        self.schema_name = schema_name
        self.sample_size = sample_size

    def infer_logical_type(self, dtype):
        dtype = dtype.lower()
        if dtype in ['integer', 'bigint', 'smallint', 'numeric', 'real', 'double precision']:
            return "numeric"
        elif dtype in ['boolean']:
            return "numeric"
        elif dtype in ['timestamp without time zone', 'timestamp with time zone', 'date']:
            return "datetime"
        elif dtype in ['character varying', 'text', 'character']:
            return "string"
        return "categorical"

    def _get_numeric_stats(self, cursor, table, column):

        cursor.execute(f'''
            SELECT COUNT(DISTINCT {column}) AS num_unique,
                   MIN({column}) AS min_val,
                   MAX({column}) AS max_val,
                   PERCENTILE_CONT(ARRAY[0.0, 0.25, 0.5, 0.75, 1.0]) 
                   WITHIN GROUP (ORDER BY {column}) AS percentiles
            FROM "{self.schema_name}"."{table}"
            WHERE {column} IS NOT NULL
        ''')

        return cursor.fetchone()

    def _get_sample_values(self, cursor, table, column, all_values=False):
        limit = "" if all_values else "LIMIT 10"
        cursor.execute(f'''
            SELECT DISTINCT {column}
            FROM "{self.schema_name}"."{table}"
            WHERE {column} IS NOT NULL
            {limit}
        ''')
        return [row[column] for row in cursor.fetchall()]

    def extract_schema(self):
        conn = psycopg2.connect(**self.db_config)
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        output = {"tables": {}}

        cursor.execute(f'''
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type = 'BASE TABLE' AND table_schema = %s
        ''', (self.schema_name,))
        tables = cursor.fetchall()

        for table_info in tables:
            table_name = table_info['table_name']
            cursor.execute(f'SELECT COUNT(*) FROM "{self.schema_name}"."{table_name}"')
            num_rows = cursor.fetchone()['count']

            cursor.execute(f'''
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
            ''', (self.schema_name, table_name))
            columns = cursor.fetchall()

            table_result = {
                "num_rows": num_rows,
                "columns": {}
            }

            for col in columns:
                col_name = col['column_name']
                col_type = col['data_type']
                logical_type = self.infer_logical_type(col_type)
                # print("-----------",cursor, table_name, col_name, logical_type)

                if logical_type == "numeric":
                    samples = self._get_sample_values(cursor, table_name, col_name)
                    stats = self._get_numeric_stats(cursor, table_name, col_name)
                    # print("-----------",cursor, table_name, col_name, logical_type, samples, stats)
                    
                    col_result = {
                        "num_unique": stats["num_unique"],
                        "sample_values": samples,
                        "logical_type": logical_type,
                        "range": {"min": stats["min_val"], "max": stats["max_val"]},
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
                    samples = self._get_sample_values(cursor, table_name, col_name)
                    col_result = {
                        "num_unique": len(samples),
                        "sample_values": samples,
                        "logical_type": logical_type
                    }
                    if logical_type == "datetime":
                        cursor.execute(f'''
                            SELECT MIN({col_name}) AS min_val, MAX({col_name}) AS max_val
                            FROM "{self.schema_name}"."{table_name}"
                            WHERE {col_name} IS NOT NULL
                        ''')
                        result = cursor.fetchone()
                        col_result["range"] = {"min": result["min_val"], "max": result["max_val"]}

                table_result["columns"][col_name] = col_result

            output["tables"][f"{self.schema_name}.{table_name}"] = table_result

        output["source"] = "database"
        return output
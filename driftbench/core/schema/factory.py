from .csv_extractor import CSVSchemaExtractor
from .postgres_extractor import PostgresSchemaExtractor

def get_schema_extractor(source_type: str, **kwargs):
    if source_type == "csv":
        return CSVSchemaExtractor(**kwargs)
    elif source_type == "postgres":
        return PostgresSchemaExtractor(**kwargs)
    else:
        raise ValueError(f"Unsupported source_type: {source_type}")

import os
from pathlib import Path

DB_DIR = str(Path(__file__).parent.parent.parent.parent / "data")


class Metadata:
    def __init__(self, schemas: str):
        self.name = schemas.split("/")[-1]
        self.data: dict[str, dict[str, str]] = {}
        self._load_metadata(os.path.join(DB_DIR, schemas, "metadata.yaml"))

    def __str__(self):
        result = []
        for table, columns in self.data.items():
            result.append(f"[{table}]")
            for col, dtype in columns.items():
                result.append(f"{col}={dtype}")
        return "\n".join(result)
    
    def _load_metadata(self, metadata_path: str):
        import yaml
        try:
            with open(metadata_path, "r") as f:
                content = yaml.safe_load(f)
                for table in content.get("tables", []):
                    table_name = table.get("table_name", "").lower()
                    table_meta = {}
                    for column in table.get("columns", []):
                        column_name = column.get("column_name", "").strip()
                        column_type = column.get("column_type", "").strip()
                        if column_name and column_type:
                            table_meta[column_name] = column_type
                    if table_name:
                        self.data[table_name] = table_meta
        except FileNotFoundError:
            raise FileNotFoundError(f"{self.name} schema not found.")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing {self.name} schema: {e}")
                

    def get_table(self, table_name: str) -> dict[str, str]:
        return self.data.get(table_name, {})
    
    
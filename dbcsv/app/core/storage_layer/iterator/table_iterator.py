

from pathlib import Path
import csv
import os
import json
from typing import List, Any

from dbcsv.app.core.storage_layer.datatypes import prepare_converters

DB_DIR = str(Path(__file__).parent.parent.parent.parent.parent / "data")

class TableIterator:
    def __init__(self, db: str, table: str, metadata: dict[str, str], batch_size: int = 1000):
        self.__db = db.lower()
        self.__table = table.lower()
        self.__batch_size = batch_size
        self.__columns = list(metadata.keys())
        self.__column_types = list(metadata.values())
        self.__converters = prepare_converters(self.__column_types)
        self.__file = self.__load_file(db=self.__db, table=self.__table)
        self.__reader = csv.reader(self.__file)
        self.__check_header()
        self.__is_done = False
        self.__cache: List[List[Any]] = []
        self.__used: List[List[Any]] = []
    
    def __iter__(self) -> 'TableIterator':
        return self
    
    def _load_next_batch(self) -> List[List[Any]]:
        """
        Load up to batch_size rows into self.__cache.
        Any row that raises ValueError during conversion or has wrong length is skipped.
        """
        self.__cache = []
        while len(self.__cache) < self.__batch_size:
            try:
                raw_row = next(self.__reader)
            except StopIteration:
                self.__is_done = True
                break

            # Attempt to convert the row
            try:
                converted = [fn(cell) for fn, cell in zip(self.__converters, raw_row)]
                # If the row is too short or too long, skip it
                if len(converted) != len(self.__columns):
                    continue
            except ValueError:
                # Conversion failed (invalid int/float/bool/date/etc.) → skip row
                # Optionally log warning here
                continue

            # If we reach here, row is valid
            self.__cache.append(converted)
    
    def __next__(self) -> List[List[Any]]:
        if not self.__cache and not self.__is_done:
            self._load_next_batch()

        if self.__cache:
            self.__used.append(self.__cache[0])
            return self.__cache.pop(0)
        else:
            self.close()
            raise StopIteration       


    def __load_file(self, db: str, table: str):
        db, table = db.lower(), table.lower()
        data_path = os.path.join(DB_DIR, db, table + ".csv")
        # print(f"Loading data from {data_path}")
        try:
            return open(data_path, "r", encoding="utf-8")
        except FileNotFoundError:
            print(self.__table)
            raise FileNotFoundError(f"Table {self.__table} not found.")
        except Exception as e:
            raise Exception(f"Error loading table {self.__table}: {e}")
        
    def __check_header(self) -> None:
        header = next(self.__reader)
        if len(header) != len(self.__columns):
            raise ValueError(f"Header length does not match column length in {self.__db}/{self.__table}.")
        if any(col.lower() != header[i].lower() for i, col in enumerate(self.__columns)):
            raise ValueError(f"Header names do not match column names in {self.__db}/{self.__table}.")

    def __del__(self):
        self.close()

    def __repr__(self):
        result = f"Table: {self.__table}\n"
        columns = [f"\n\t{col} ({typ})" for col, typ in zip(self.__columns, self.__column_types)]
        result += f"Columns: {''.join(columns)}\n"
        
        col_widths = [max(len(str(cell)) for cell in [col] + [row[i] for row in self._data]) for i, col in enumerate(self.__columns)]

        header = [h.ljust(width) for h, width in zip(self.__columns, col_widths)]
        result += " | ".join(header) + "\n"
        result += "-" * (sum(col_widths) + 3 * (len(header) - 1)) + "\n"

        for row in self.__cache:
            row_str = [str(cell).ljust(width) for cell, width in zip(row, col_widths)]
            result += " | ".join(row_str) + "\n"

        return result
    
    def close(self) -> None:
        if hasattr(self, "_file") and self.__file:
            self.__file.close()
    
    def to_json(self, limit: int = None):
        if not limit:
            limit = self.__batch_size

        tmp_file = self.__load_file(__db=self.__db, table=self.__table)
        tmp_reader = csv.reader(tmp_file)
        next(tmp_reader)  # Skip the header

        data = []
        for i, row in enumerate(tmp_reader):
            if i >= limit:
                break
            if len(row) != len(self.__columns):
                raise ValueError(f"Row length does not match column length in {self.__db}/{self.__table}.")
            data.append(row)

        tmp_file.close()

        result = {
            "__table": self.__table,
            "columns": self.__columns,
            "column_types": self.__column_types,
            "data": data
        }
        return json.dumps(result, indent=4)

    @property
    def cache(self):
        return self.__cache

    @property
    def columns(self):
        return self.__columns

    @property
    def column_types(self):
        return self.__column_types

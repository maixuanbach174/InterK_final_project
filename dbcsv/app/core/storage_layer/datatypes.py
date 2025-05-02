import datetime

class DBTypeObject:
    def __init__(self, *values):
        self.values = values

    def __eq__(self, other):
        if other in self.values:
            return True
        return False
    
    @staticmethod
    def convert_datatype(data: str, dtype: str = "") -> any:
        if dtype.lower() == STRING:
            if data.startswith("'") and data.endswith("'"):
                return data[1:-1]
            return data
        elif data.startswith("'") and data.endswith("'") and dtype.lower() != STRING:
            raise ValueError(f"Invalid {dtype.lower} format: {data} is a string, not a {dtype.lower()}")
        elif dtype.lower() == INTEGER:
            try:
                return int(data)
            except ValueError:
                raise ValueError(f"Invalid integer format: {data} is not an integer")
        elif dtype.lower() == FLOAT:
            try:
                return float(data)
            except ValueError:
                raise ValueError(f"Invalid float format: {data} is not a float")
        elif dtype.lower() == BOOLEAN:
            if data.lower() in ["true", "false"]:
                return data.lower() == "true"
            else:
                raise ValueError(f"Invalid boolean format: {data} is not a boolean value")
        elif dtype.lower() == DATE or dtype.lower() == DATETIME:
            try:
                return datetime.datetime.strptime(data, "%Y-%m-%d").date()
            except ValueError:
                raise ValueError(f"Invalid date format (format %Y-%m-%d): {data}")
        elif dtype.lower() == NULL:
            if data.lower() == "null":
                return None
            else:
                raise ValueError(f"Invalid null format: {data}")
        else:
            try:
                if data.startswith("'") and data.endswith("'"):
                    return data[1:-1]
                else:
                    try:
                        return int(data)
                    except ValueError:
                        try:
                            return float(data)
                        except ValueError:
                            return data
            except Exception as e:
                raise ValueError(f"Invalid data format: {data}") from e
            
    @staticmethod
    def convert_type(row: list[str], column_types: list[str]) -> list[any]:
        if len(row) != len(column_types):
            raise ValueError(f"Row length {len(row)} does not match column types length {len(column_types)}")
        return [DBTypeObject.convert_datatype(data, dtype) for data, dtype in zip(row, column_types)]
    def __repr__(self):
        if "VARCHAR" in self.values:
            return "STRING_TYPE"
        else: 
            return "NUMERIC_TYPE"
            

# STRING = DBTypeObject("varchar", "text", "char")
# INTEGER = DBTypeObject("integer", "int", "bigint", "smallint", "tinyint")
# FLOAT = DBTypeObject("float", "double", "decimal", "dec")
# BOOLEAN = DBTypeObject("boolean", "bool")
# DATE = DBTypeObject("date")
# DATETIME = DBTypeObject("datetime", "timestamp")
# NULL = DBTypeObject("null")
# STRING_TYPE = DBTypeObject("varchar", "text", "char", "datetime", "timestamp", "date")
# NUMERIC_TYPE = DBTypeObject("integer", "int", "bigint", "smallint", "tinyint", "float", "double", "decimal", "dec")

# Convert them to uppercase
STRING = DBTypeObject("VARCHAR", "TEXT", "CHAR")
INTEGER = DBTypeObject("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT")
FLOAT = DBTypeObject("FLOAT", "DOUBLE", "DECIMAL", "DEC")
BOOLEAN = DBTypeObject("BOOLEAN", "BOOL")
DATE = DBTypeObject("DATE")
DATETIME = DBTypeObject("DATETIME", "TIMESTAMP")
NULL = DBTypeObject("NULL")
STRING_TYPE = DBTypeObject("VARCHAR", "TEXT", "CHAR", "DATETIME", "TIMESTAMP", "DATE")
NUMERIC_TYPE = DBTypeObject("INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "DEC")

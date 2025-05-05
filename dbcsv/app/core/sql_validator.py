import operator
from typing import Any, Callable, List
from sqlglot import TokenError, parse_one, exp
from sqlglot.errors import ParseError
from dbcsv.app.core.storage_layer.metadata import Metadata
from dbcsv.app.core.storage_layer.datatypes import convert_datatype

class SQLValidator:
    QUOTE_TYPES = {
        "VARCHAR", "TEXT", "CHAR", "STRING", "DATE", "DATETIME", "TIMESTAMP"
    }
    STRING_TYPES = {
        "VARCHAR", "TEXT", "CHAR", "STRING"
    }
    NUMERIC_TYPES = {
        "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL", "DEC", "BOOLEAN", "BOOL"
    }
    OPERATORS = {
        exp.EQ:  operator.eq,
        exp.NEQ: operator.ne,
        exp.LT:  operator.lt,
        exp.LTE: operator.le,
        exp.GT:  operator.gt,
        exp.GTE: operator.ge,
    }

    def __init__(self, metadatas: dict[str, Metadata], dialect: str = "mysql"):
        self.__metadatas = metadatas
        self.__dialect = dialect

    def parse(self, sql: str) -> exp.Expression:
        try:
            return parse_one(sql, read=self.__dialect)
        except (ParseError, TokenError) as e:
            raise SyntaxError(f"Syntax error: {e}")

    def validate(self, tree: exp.Expression, db: str) -> dict[str, Any]:
        # 1. Only SELECT
        if not isinstance(tree, exp.Select):
            raise SyntaxError("Only SELECT statements are supported")
        
        if db not in self.__metadatas:
            raise ValueError(f"Database not found: {db}")
        
        metadata = self.__metadatas.get(db)

        # 2. FROM clause: only one table
        tables = list(tree.find_all(exp.Table))

        if len(tables) != 1:
            raise SyntaxError("Only one table is supported in FROM clause")
        
        # not allow allias for the table
        table = tables[0].name
        if tables[0].alias:
            raise SyntaxError("Table alias is not supported")
        
        if tables[0].db and tables[0].db != db:
            raise ValueError(f"Invalid database qualifier for \'{table}\': \'{tables[0].db}\'")
        # check if the table exists in the metadata
        if table not in metadata.data:
            raise ValueError(f"Table '{table}' not found in database '{db}'")

        # 3. Projections: only *, table.*, col, table.col, or a comma-separated list thereof
        columns = self._validate_projection(tree, table, db, metadata)
        # 4. WHERE clause (unchanged)
        where = tree.args.get("where")
        predicate = None
        always_true = False
        always_false = False

        if where:
            expr = where.this
            # first, attempt constant‑fold
            const = self._eval_constant(expr)
            if const is True:
                # no need to filter at all
                always_true = True
                predicate = None
            elif const is False:
                # filter always rejects → nothing to scan
                always_false = True
                # we can assign a lambda that never passes, or handle specially later
                predicate = lambda row, cols: False
            else:
                # non‑constant: build the usual predicate fn
                predicate = self._validate_predicate(expr, table, metadata)
        return {
            "db": db,
            "columns": columns,
            "table": table,
            "predicate": predicate,
            "always_true": always_true,
            "always_false": always_false,
        }

    def _validate_projection(self, tree: exp.Expression, table: str, db: str, metadata: Metadata) -> list[str]:
        columns = []
        
        if not tree.expressions:
            raise SyntaxError("No projection specified")
        for projection in tree.expressions:
            if isinstance(projection, exp.Alias):
                raise SyntaxError(f"Alias is not supported '{projection}")
            elif isinstance(projection, exp.Column):  
                if projection.catalog:
                    raise SyntaxError(f"Catalog is not supported '{projection}")
                if projection.alias:
                    raise SyntaxError(f"Alias is not supported '{projection}")
                if projection.expressions:
                    raise SyntaxError(f"Expression is not supported '{projection}")
                if projection.db and projection.db != db:
                    raise ValueError(f"Invalid database qualifier for '{projection.name}': '{projection.db}'")
                if projection.table and projection.table != table:
                    raise ValueError(f"Invalid table qualifier for '{projection.name}': '{projection.table}'")
                if projection.name not in metadata.data[table] and projection.name != "*":
                    raise ValueError(f"Column '{projection.name}' not found in table '{table}'")
                columns.append(projection.name)
            elif isinstance(projection, exp.Star):
                columns.append("*")
            else:
                raise SyntaxError(f"Unsupported projection: {projection.sql()}")
        return columns
        

    def _validate_predicate(self, expr: exp.Expression, table: str, metadata: Metadata) -> Callable[[List[Any], List[str]], bool]:
        if isinstance(expr, exp.Boolean):
            return lambda row, cols: expr.this
        
        if isinstance(expr, exp.Paren):
            return self._validate_predicate(expr.this, table, metadata)
        # AND / OR
        if isinstance(expr, exp.And):
            left_fn  = self._validate_predicate(expr.left,  table, metadata)
            right_fn = self._validate_predicate(expr.right, table, metadata)
            return lambda row, cols: left_fn(row, cols) and right_fn(row, cols)

        if isinstance(expr, exp.Or):
            left_fn  = self._validate_predicate(expr.left,  table, metadata)
            right_fn = self._validate_predicate(expr.right, table, metadata)
            return lambda row, cols: left_fn(row, cols) or right_fn(row, cols)

        # comparison
        if isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)):
            op_fn = self.OPERATORS[type(expr)]

            # Column vs Literal
            if isinstance(expr.left, exp.Column) and isinstance(expr.right, exp.Literal):
                col_type = self._get_column_type(expr.left, table, metadata)
                right = expr.right.name
                if expr.right.is_int:
                    right = int(right)
                    if col_type not in self.QUOTE_TYPES:
                        return lambda row, cols: op_fn(row[cols.index(expr.left.name)], right)
                    else: 
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
                if expr.right.is_number:
                    right = float(right)
                    if col_type not in self.QUOTE_TYPES:
                        return lambda row, cols: op_fn(row[cols.index(expr.left.name)], right)
                    else: 
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
                if col_type in self.QUOTE_TYPES:
                    try:
                        right = convert_datatype(right, col_type)
                        return lambda row, cols: op_fn(row[cols.index(expr.left.name)], right)   
                    except ValueError:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
                raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")

            # Column vs Column
            if isinstance(expr.left, exp.Column) and isinstance(expr.right, exp.Column):
                t1 = self._get_column_type(expr.left, table, metadata)
                t2 = self._get_column_type(expr.right, table, metadata)
                if t1 in self.NUMERIC_TYPES and t2 in self.NUMERIC_TYPES:
                    return lambda row, cols: op_fn(
                        row[cols.index(expr.left.name)],
                        row[cols.index(expr.right.name)]
                    )
                if t1 in self.STRING_TYPES and t2 in self.STRING_TYPES:
                    return lambda row, cols: op_fn(
                        row[cols.index(expr.left.name)],
                        row[cols.index(expr.right.name)]
                    )
                if t1 != t2:
                    raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
                return lambda row, cols: op_fn(
                        row[cols.index(expr.left.name)],
                        row[cols.index(expr.right.name)]
                    )
            # Literal vs Literal
            if isinstance(expr.left, exp.Literal) and isinstance(expr.right, exp.Literal):
                if expr.left.is_number and expr.right.is_number:
                    return lambda row, cols: op_fn(float(expr.left.name), float(expr.right.name))
                elif expr.left.is_string and expr.right.is_string:
                    return lambda row, cols: op_fn(expr.left.name, expr.right.name)
                else:
                    raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")

            # Literal vs Column
            if isinstance(expr.left, exp.Literal) and isinstance(expr.right, exp.Column):
                col_type = self._get_column_type(expr.right, table, metadata)
                left = expr.left.name
                if expr.left.is_int:
                    left = int(left)
                    if col_type not in self.QUOTE_TYPES:
                        return lambda row, cols: op_fn(left, row[cols.index(expr.right.name)])
                    else:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
                if expr.left.is_number:
                    left = float(left)
                    if col_type not in self.QUOTE_TYPES:
                        return lambda row, cols: op_fn(left, row[cols.index(expr.right.name)])
                    else:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
                if col_type in self.QUOTE_TYPES:
                    try:
                        left = convert_datatype(left, col_type)
                        return lambda row, cols: op_fn(left, row[cols.index(expr.right.name)] )   
                    except ValueError:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
                raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")

            if isinstance(expr.left, exp.Boolean):
                if isinstance(expr.right, exp.Column):
                    if self._get_column_type(expr.right, table, metadata).upper() in self.QUOTE_TYPES:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}") 
                    return lambda row, cols: op_fn(expr.left.this,  row[cols.index(expr.right.name)])
                elif isinstance(expr.right, exp.Boolean):
                    return lambda row, cols: op_fn(expr.left.this, expr.right.this)
                elif isinstance(expr.right, exp.Literal):
                    if expr.right.is_number:
                        return lambda row, cols: op_fn(expr.left.this, float(expr.right.name))
                    else:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")  
            
            if isinstance(expr.right, exp.Boolean):
                if isinstance(expr.left, exp.Column):
                    if self._get_column_type(expr.left, table, metadata).upper() in self.QUOTE_TYPES:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}") 
                    return lambda row, cols: op_fn(row[cols.index(expr.left.name)], expr.right.this)
                elif isinstance(expr.left, exp.Boolean):
                    return lambda row, cols: op_fn(expr.left.this, expr.right.this)
                elif isinstance(expr.left, exp.Literal):
                    if expr.left.is_number:
                        return lambda row, cols: op_fn(float(expr.left.name), expr.right.this)
                    else:
                        raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")   
            

        raise SyntaxError(f"Unsupported predicate: {expr.sql()}")


    def _get_column_type(self, column: exp.Expression, table: str, metadata: Metadata) -> str:
      
        if isinstance(column, exp.Column):
            if column.catalog:
                raise SyntaxError(f"Catalog not supported: {column.sql()}")
            if column.db and column.db != metadata.name:
                raise ValueError(f"Invalid database qualifier: {column.sql()}")
            if column.table and column.table != table:
                raise ValueError(f"Invalid table qualifier: {column.sql()}")
            
            if column.name not in metadata.data[table]:
                raise ValueError(f"Column not found: {column.sql()}")
            return metadata.data[table][column.name]
        raise ValueError(f"Unsupported Column type: {column.sql()}")  

    def _eval_constant(self, expr: exp.Expression) -> bool | None:
        """
        Return True/False if expr contains no Column and can be evaluated now;
        otherwise return None.
        """
        # if there is any Column anywhere, it’s non‑constant
        if isinstance(expr, exp.Paren):
            return self._eval_constant(expr.this)

        if expr.find(exp.Column):
            return None

        # Literal boolean (WHERE TRUE, WHERE FALSE)
        if isinstance(expr, exp.Boolean):
            return bool(expr.this)

        # comparison of two literals
        if isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)):
            left, right = expr.left, expr.right
            if isinstance(left, exp.Literal) and isinstance(right, exp.Literal):
                # convert to Python values
                if expr.left.is_number and expr.right.is_number:
                    l = float(expr.left.name)
                    r = float(expr.right.name)
                    return self.OPERATORS[type(expr)](l, r)
                elif expr.left.is_string and expr.right.is_string:
                    return self.OPERATORS[type(expr)](expr.left, expr.right)
                else:
                    raise TypeError(f"Type mismatch: {expr.left.sql()} vs {expr.right.sql()}")
            return None

        # AND/OR: both sides must be constant
        if isinstance(expr, exp.And):
            l = self._eval_constant(expr.left)
            r = self._eval_constant(expr.right)
            if l is not None and r is not None:
                return l and r
            return None

        if isinstance(expr, exp.Or):
            l = self._eval_constant(expr.left)
            r = self._eval_constant(expr.right)
            if l is not None and r is not None:
                return l or r
            return None

        # unhandled node types → non‑constant
        return None   

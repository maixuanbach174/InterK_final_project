from sqlglot import parse_one, exp
from sqlglot.errors import ParseError
from dbcsv.app.core.storage_layer.metadata import Metadata

class SQLValidator:
    NUMERIC_TYPES = {
        "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT",
        "FLOAT", "DOUBLE", "DECIMAL", "DEC"
    }
    STRING_TYPES = {
        "VARCHAR", "TEXT", "CHAR", "STRING"
    }
    def __init__(self, metadatas: dict[str, Metadata], dialect: str = "mysql"):
        self.__metadatas = metadatas
        self.__dialect = dialect

    def parse(self, sql: str) -> exp.Expression:
        try:
            return parse_one(sql, read=self.__dialect)
        except ParseError as e:
            raise SyntaxError(f"Syntax error: {e}")

    def validate(self, tree: exp.Expression, db: str):
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
        self._validate_projection(tree, table, db, metadata)
        # 4. WHERE clause (unchanged)
        where = tree.args.get("where")
        if where:
            self._validate_predicate(where.this, table, metadata)

    def _validate_projection(self, tree: exp.Expression, table: str, db: str, metadata: Metadata):
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
            elif isinstance(projection, exp.Star):
                continue
            else:
                raise SyntaxError(f"Unsupported projection: {projection.sql()}")
        

    def _validate_predicate(self, expr: exp.Expression, table: str, metadata: Metadata):
        """Validate WHERE clause predicates with type checking."""
        # Handle logical operators (AND/OR)
        if isinstance(expr, (exp.And, exp.Or)):
            self._validate_predicate(expr.left, table, metadata)
            self._validate_predicate(expr.right, table, metadata)
            return

        # Handle comparison operators (=, <, >, etc.)
        if isinstance(expr, (exp.EQ, exp.NEQ, exp.GT, exp.LT, exp.GTE, exp.LTE)):
            left_type = self._get_operand_type(expr.left, table, metadata)
            right_type = self._get_operand_type(expr.right, table, metadata)

            if left_type in self.NUMERIC_TYPES and right_type in self.NUMERIC_TYPES:
                return
            if left_type in self.STRING_TYPES and right_type in self.STRING_TYPES:
                return
            if left_type == right_type:
                return
            raise TypeError(f"Type mismatch: {left_type} vs {right_type} in {expr.sql()}")

        raise SyntaxError(f"Unsupported predicate: {expr.sql()}")

    def _get_operand_type(self, operand: exp.Expression, table: str, metadata: Metadata) -> str:
      
        if isinstance(operand, exp.Column):
            if operand.catalog:
                raise SyntaxError(f"Catalog not supported: {operand.sql()}")
            if operand.db and operand.db != metadata.name:
                raise ValueError(f"Invalid database qualifier: {operand.sql()}")
            if operand.table and operand.table != table:
                raise ValueError(f"Invalid table qualifier: {operand.sql()}")
            
            if operand.name not in metadata.data[table]:
                raise ValueError(f"Column not found: {operand.sql()}")
            return metadata.data[table][operand.name]
        
        elif isinstance(operand, exp.Literal):
            # Infer basic types from literals
            if operand.is_string:
                return "VARCHAR"
            elif operand.is_int:
                return "INTEGER"
            elif operand.is_number:
                return "FLOAT"
    
        raise ValueError(f"Unsupported literal type: {operand.sql()}")     
from sqlglot import parse_one, exp
from dbcsv.app.core.sql_validator import SQLValidator, exp
from dbcsv.app.core.storage_layer.metadata import Metadata

tests = [
    # valid
#    ("SELECT id + age FROM table1",           False, SyntaxError),
#     ("SELECT age*2, id FROM table1",          False, SyntaxError),
#     ("SELECT (id + age) FROM table1",         False, SyntaxError),

#     # function calls
#     ("SELECT COUNT(id) FROM table1",          False, SyntaxError),
#     ("SELECT MAX(age), MIN(age) FROM table1", False, SyntaxError),

#     # aliases in projection
#     ("SELECT id AS x FROM table1",            False, SyntaxError),
#     ("SELECT name x FROM table1",             False, SyntaxError),

#     # mixing allowed and disallowed
#     ("SELECT *, id+age FROM table1",          False, SyntaxError),
#     ("SELECT table1.*, id+age FROM table1",   False, SyntaxError),

#     # literal in projection
#     ("SELECT 'constant' FROM table1",         False, SyntaxError),
#     ("SELECT 123, id FROM table1",            False, SyntaxError),

#     # wildcard with wrong qualifier
#     ("SELECT db1.* FROM table1",              False, SyntaxError),
#     ("SELECT other.* FROM table1",            False, SyntaxError),

    # valid SELECT/FROM/WHERE and simple projections
    ("SELECT * FROM table1 WHERE 30 > age",             True,  None),
    ("SELECT id, name, age FROM table1 WHERE age < 100.0", True, None),
    ("SELECT * FROM table1 LIMIT 5",                    True,  None),  # if LIMIT is allowed
    ("SELECT COUNT(*) FROM table1",                     True,  None),  # if aggregates pass

    # projection‑only valid forms
    ("SELECT * FROM table1 WHERE id > 1 AND age < 30",                            True,  None),
    ("SELECT id FROM table1 WHERE age = id",                           True,  None),
    ("SELECT table1.* FROM table1 WHERE 1 = 1",                     True,  None),
    ("SELECT db1.table1.* FROM db1.table1 WHERE db1.table1.name = 'bach'",                 True,  None),
    ("SELECT table1.id FROM table1 WHERE table1.name = table1.name",                    True,  None),
    ("SELECT db1.table1.id FROM table1 WHERE db1.table1.id > 1 OR table1.id < 30",                True,  None),
    ("SELECT *, id, table1.age, db1.table1.name FROM table1 WHERE (30 > id OR age > 30) AND name = 'Bach'", True, None),

    # # syntax‑level failures
    # ("SELEC id FROM table1",       False, SyntaxError),
    # ("SELECT FROM table1",         False, SyntaxError),
    # ("SELECT * table1",            False, SyntaxError),
    # ("SELECT * FROM table1 WHERE age >> 30", False, SyntaxError),

    # # FROM‑clause failures
    # ("SELECT * FROM table1, table2", False, SyntaxError),
    # ("SELECT * FROM table1 t",       False, SyntaxError),
    # ("SELECT * FROM otherdb.table1", False, ValueError),
    # ("SELECT * FROM unknown_table",  False, ValueError),

    # # projection failures
    # ("SELECT id AS id2 FROM table1",   False, SyntaxError),
    # ("SELECT id + age FROM table1",    False, SyntaxError),
    # ("SELECT foo FROM table1",          False, ValueError),
    # ("SELECT table2.id FROM table1",   False, ValueError),

    # # projection pattern failures
    # ("SELECT db1.* FROM table1",        False, SyntaxError),
    # ("SELECT other.* FROM table1",      False, SyntaxError),
    # ("SELECT db1.table1.col1+col2 FROM table1", False, SyntaxError),
    # ("SELECT COUNT(id) FROM table1",    False, SyntaxError),
    # ("SELECT (id) FROM table1",         False, SyntaxError),
    # ("SELECT id x FROM table1",         False, SyntaxError),

    # WHERE‑predicate failures
    # ("SELECT * FROM table1 WHERE age BETWEEN 20 AND 30", False, SyntaxError),
    # ("SELECT * FROM table1 WHERE name LIKE 'A%'",       False, SyntaxError),
    # ("SELECT * FROM table1 WHERE 'abc' > age",          False, ValueError),
    # ("SELECT * FROM table1 WHERE name > age",           False, ValueError),
    # ("SELECT * FROM table1 WHERE age + 1 > 10",         False, ValueError),
    # ("SELECT * FROM table1 WHERE age",                  False, SyntaxError),
    # ("SELECT * FROM table1 WHERE age > TRUE",           False, ValueError),

    # # non‑SELECT
    # ("INSERT INTO table1 (id) VALUES (1)", False, SyntaxError),
]


if __name__ == "__main__":
    # Example usage
    sql = "SELECT * FROM table1 WHERE 30 > age"
    metadatas = {
        'db1': Metadata('db1'),
        'db2': Metadata('db2')
    }

    # validator = SQLValidator(metadatas=metadatas)
    # result = validator.parse(sql)
    # expr = result.args.get("where").this.left
    # print(type(expr.name))
    

    for test in tests:
        sql, expected_result, expected_exception = test
        validator = SQLValidator(metadatas)
        try:
            tree = validator.parse(sql)
            # print(validator.validate(tree, 'db1'))
            print(repr(tree))
        except Exception as e:
            result = False
            print(f"Error {e}: {sql}: ")
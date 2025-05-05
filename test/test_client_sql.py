import sys
import json
from dbapi2.src.dbcsv.connection import connect, ProgrammingError


# your server + credentials
BASE_URL = "http://127.0.0.1:80"
USERNAME = "johndoe"
PASSWORD = "secret"
DB = "db1"

# (sql, expected_rowcount, expected_exception)
tests = [
    # simple column vs column
    ("SELECT * FROM table4 WHERE student_id > gpa", 100, None),
    ("SELECT * FROM table4 WHERE gpa < student_id", 100, None),
    ("SELECT * FROM table4 WHERE student_id = gpa", 100, None),

    # column vs literal (and literal vs column)
    ("SELECT * FROM table4 WHERE student_id > 5", 100, None),
    ("SELECT * FROM table4 WHERE 5 < student_id", 100, None),
    ("SELECT * FROM table4 WHERE gpa >= 3.5", 100, None),

    # constant predicates
    ("SELECT * FROM table4 WHERE 1 = TRUE", 100, None),    # always false
    ("SELECT * FROM table4 WHERE TRUE", 100, None),       # always true
    ("SELECT * FROM table4 WHERE FALSE", 100, None),

    # boolean column
    ("SELECT * FROM table4 WHERE is_enrolled = TRUE", 100, None),
    ("SELECT * FROM table4 WHERE FALSE = is_enrolled", 100, None),

    # date comparisons
    ("SELECT * FROM table4 WHERE birth_date > '2000-01-01'", 100, None),
    ("SELECT * FROM table4 WHERE '2000-02-29' = birth_date", 100, None),

    # mixed logicals
    ("SELECT * FROM table4 WHERE is_enrolled = TRUE AND gpa > 3.0", 100, None),
    ("SELECT * FROM table4 WHERE (student_id > 5 OR birth_date > '2000-01-01') AND 1 = TRUE", 100, None),

    # parentheses precedence
    (
      "SELECT * FROM table4 WHERE student_id > 5 OR birth_date > '2000-01-01' AND gpa > 3.0",
      100, None
    ),
    (
      "SELECT * FROM table4 WHERE (student_id > 5 OR birth_date > '2000-01-01') AND gpa > 3.0",
      100, None
    ),

    # projection variants
    # ("SELECT * FROM table4 WHERE student_id < 3", 100, None),
    # ("SELECT table4.* FROM table4 WHERE student_id < 3", 100, None),
    # ("SELECT db1.table4.* FROM table4 WHERE student_id < 3", 100, None),
    # ("SELECT student_id FROM table4 WHERE student_id < 3", 100, None),
    # ("SELECT db1.table4.student_id FROM table4 WHERE student_id < 3", 100, None),

    # error cases: expect ProgrammingErrorxw
    ("SELECT student_id AS id FROM table4 WHERE TRUE", None, ProgrammingError),
    ("SELECT student_id+gpa FROM table4 WHERE TRUE", None, ProgrammingError),
    ("SELECT * FROM table4 WHERE student_id BETWEEN 1 AND 5", None, ProgrammingError),
    ("SELECT * FROM table4 WHERE foo = 1", None, ProgrammingError),
    ("SELECT * FROM table4 WHERE other.id = 1", None, ProgrammingError),
]

def main():
    try:
        conn = connect(BASE_URL, USERNAME, PASSWORD, DB)
    except Exception as e:
        print(f"CONNECT FAILED: {e}")
        sys.exit(1)

    cur = conn.cursor()
    passed = failed = 0

    for sql, expected_count, expected_exc in tests:
        try:
            cur.execute(sql)
            if expected_exc:
                print(f"❌ FAIL: {sql!r} expected exception {expected_exc.__name__} but got no exception")
                failed += 1
            else:
                rows = cur.fetchmany(100)
                got = len(rows)
                if got == expected_count:
                    print(f"✅ PASS: {sql!r} -> {got} rows")
                    passed += 1
                else:
                    print(f"❌ FAIL: {sql!r} -> expected {expected_count} rows, got {got}")
                    failed += 1
        except Exception as e:
            if expected_exc and isinstance(e, expected_exc):
                print(f"✅ PASS: {sql!r} raised {e.__class__.__name__}")
                passed += 1
            else:
                print(f"❌ FAIL: {sql!r} raised unexpected {e.__class__.__name__}: {e}")
                failed += 1

    cur.close()
    conn.close()
    print(f"\nSummary: {passed} passed, {failed} failed out of {len(tests)} tests")

if __name__ == "__main__":
    main()

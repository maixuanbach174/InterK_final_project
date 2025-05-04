from dbapi2.src.dbcsv.connection import connect

# 1) Connect (will call your /connect endpoint)
conn = connect("http://127.0.0.1:80", username="johndoe", password="secret", db="db1")

# 2) Create a cursor and execute a query (carries the Bearer token)
cur = conn.cursor()
cur.execute("SELECT db1.table3.* FROM table3 WHERE (student_id > gpa OR birth_date > '2000-01-01') AND 1 = TRUE")

# 3) Fetch results
for row in cur.fetchmany(10):
    print(row)

cur.close()
conn.close()

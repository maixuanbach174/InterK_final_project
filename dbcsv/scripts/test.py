import dbcsv
import time


def test_connection():
    conn = dbcsv.connect(
        dsn="http://intern-dbcsv.inter-k.com/schema1", user="johndoe", password="secret"
    )

    try:
        print("1. Kết nối thành công, token đang còn hạn")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM table1 WHEREEE id = 1 Where  name = 10")
        print("   -> Query thành công lần 1: \n")

        result = cursor.fetchall()
        print(result)

        print("\n2. Đợi 61s cho token gần hết hạn...")
        time.sleep(61)

        print("\n3. Thực hiện query lần 2 (token đã hết/gần hết hạn)")
        cursor.execute("SELECT * FROM table1")
        print("   -> Query thành công lần 2 (token đã được refresh tự động)")

        return True
    except Exception as e:
        print(f"Lỗi: {str(e)}")
        return False


if __name__ == "__main__":
    test_connection()

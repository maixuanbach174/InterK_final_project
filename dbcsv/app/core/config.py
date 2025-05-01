from dotenv import load_dotenv
from pathlib import Path

# Lấy đường dẫn tuyệt đối của thư mục chứa file utils.py hiện tại
current_dir = Path(__file__).parent

# Đi lên 4 cấp thư mục để tới thư mục intern-dbcsv
env_path = current_dir.parent.parent.parent / ".env"
load_dotenv(env_path)

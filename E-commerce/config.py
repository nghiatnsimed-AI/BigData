"""
Cấu hình chung cho dự án
"""
import os
import sys

# Windows mặc định dùng cp1252 -> in tiếng Việt bị UnicodeEncodeError.
# Ép stdout/stderr dùng UTF-8 với errors="replace" để không crash khi in log.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ===============================
# KIỂM TRA PHIÊN BẢN PYTHON
# ===============================
# PySpark 3.5.3 chỉ hỗ trợ Python 3.8 - 3.11.
# Python 3.12 đã xóa module `distutils` -> Python Worker crash
# -> mọi UDF / pandas_udf / RDD.map đều fail với "Python worker exited unexpectedly".
if sys.version_info >= (3, 12):
    print("=" * 60)
    print("⚠️  CẢNH BÁO PHIÊN BẢN PYTHON")
    print("=" * 60)
    print(f"Đang dùng Python {sys.version.split()[0]}")
    print("PySpark 3.5.3 KHÔNG hỗ trợ Python 3.12+")
    print("Python Worker sẽ CRASH (thiếu module 'distutils')")
    print()
    print("GIẢI PHÁP: chạy bằng venv Python 3.11")
    print("  .\\.venv311\\Scripts\\python.exe <script>.py")
    print("=" * 60)
    print()

# Đường dẫn gốc
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Data paths
DATA_RAW = os.path.join(PROJECT_ROOT, "Data", "raw")
DATA_BRONZE = os.path.join(PROJECT_ROOT, "Data", "bronze")
DATA_SILVER = os.path.join(PROJECT_ROOT, "Data", "silver")
DATA_GOLD = os.path.join(PROJECT_ROOT, "Data", "gold")

# Files
NOV_FILE = os.path.join(DATA_RAW, "2019-Nov.csv")
OCT_FILE = os.path.join(DATA_RAW, "2019-Oct.csv")

# ===============================
# CẤU HÌNH SPARK DÙNG CHUNG
# ===============================
# Ghi trong file Python để mọi script tự chạy được mà không cần set biến môi trường.

def setup_spark(app_name, master="local[*]", cores=None, shuffle_partitions=8,
                driver_memory="4g", log_level="WARN", session_timezone="UTC"):
    """
    Tạo SparkSession với cấu hình đã được sửa cho Windows.

    Các lỗi đã xử lý:
      1. Thiếu winutils.exe  -> Spark không set được permission khi tạo thư mục
                                output => lỗi "setPermission" => ghi Parquet thất bại.
      2. Thiếu lệnh python3   -> Spark không spawn được Python Worker
                                => lỗi "Cannot run program python3".
      3. Sai múi giờ session   -> Spark parse timestamp theo giờ máy (GMT+07:00)
                                trong khi nguồn CSV ghi chuẩn " UTC"
                                => mọi event_time lệch 7 tiếng so với giá trị gốc.

    Args:
        app_name: tên hiển thị trên Spark UI
        master: kiểu chạy, mặc định "local[*]" (dùng hết core)
        cores: số core cụ thể (None = dùng master)
        shuffle_partitions: số partition cho các phép shuffle
        driver_memory: bộ nhớ driver
        log_level: mức log của Spark
        session_timezone: múi giờ phiên Spark. Mặc định "UTC" vì nguồn dữ liệu
            ghi timestamp theo chuẩn UTC - giữ "UTC" để parse và lưu đúng
            giá trị gốc, không bị lệch theo giờ địa phương.
    """
    # --- Fix 1: trỏ HADOOP_HOME về nơi có winutils.exe ---
    hadoop_home = r"D:\Metadata\hadoop"
    winutils = os.path.join(hadoop_home, "bin", "winutils.exe")
    if os.path.exists(winutils):
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        # PATH để Hadoop tìm được winutils
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")
    else:
        print(f"[WARN] Không tìm thấy winutils.exe tại {winutils}")
        print(f"[WARN] Spark sẽ không ghi được file Parquet. Cần cài winutils.")

    # --- Fix 2: tạo lệnh python3 trỏ về python của venv ---
    # Spark (JVM) gọi "python3" khi spawn Python Worker qua ProcessBuilder.
    # Trên Windows, ProcessBuilder KHÔNG tự resolve file .bat/.cmd khi không có shell,
    # nên phải tạo một file .exe thật. Cách chắc chắn nhất: copy python.exe -> python3.exe.
    python_exe = sys.executable
    python_dir = os.path.dirname(python_exe)
    python3_exe = os.path.join(python_dir, "python3.exe")
    if not os.path.exists(python3_exe):
        try:
            import shutil
            shutil.copy2(python_exe, python3_exe)
            print(f"[OK] Đã tạo python3.exe -> {python_exe}")
        except Exception as e:
            print(f"[WARN] Không tạo được python3.exe: {e}")

    # --- Fix 3: chỉ định interpreter cho Python Worker ---
    # PySpark 3.5.3 chỉ hỗ trợ Python 3.8 - 3.11.
    # Python 3.12 đã XÓA module `distutils` -> Python Worker crash ngay khi khởi động.
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    # Thêm thư mục python vào PATH để JVM tìm thấy python3.exe
    if python_dir not in os.environ.get("PATH", ""):
        os.environ["PATH"] = python_dir + os.pathsep + os.environ.get("PATH", "")

    from pyspark.sql import SparkSession

    builder = (SparkSession.builder
               .appName(app_name)
               .master(master)
               .config("spark.ui.enabled", "true")
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               .config("spark.driver.memory", driver_memory)
               # Múi giờ phiên: nguồn CSV ghi "2019-11-01 00:00:00 UTC".
               # Nếu để múi giờ máy (GMT+07:00), Spark sẽ parse lệch 7 tiếng
               # và cột day/hour tính theo giờ máy -> mâu thuẫn với event_time.
               .config("spark.sql.session.timeZone", session_timezone)
               # Bỏ qua bước set permission của Hadoop khi tạo thư mục/file
               .config("spark.hadoop.fs.permissions.umask-mode", "000")
               .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
               # Cho phép ghi đè có chọn lọc theo partition
               .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))

    if cores:
        builder = builder.config("spark.driver.cores", str(cores))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


# ===============================
# TIỆN ÍCH ĐO THỜI GIAN
# ===============================
def banner(title, char="=", width=60):
    """In tiêu đề có đường kẻ."""
    print("\n" + char * width)
    print(title)
    print(char * width)


def human_bytes(num):
    """Đổi số byte sang dạng người đọc được."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num) < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"


def dir_size(path):
    """Tính tổng dung lượng của thư mục (đệ quy)."""
    total = 0
    count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            if os.path.exists(fp):
                total += os.path.getsize(fp)
                count += 1
    return total, count

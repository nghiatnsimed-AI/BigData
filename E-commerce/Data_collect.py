"""
GIAI ĐOẠN 1: THU THẬP DỮ LIỆU
E-commerce Behavior Data Pipeline
"""

from config import *
import sys

# ===============================
# 2. KHỞI TẠO SPARK SESSION
# ===============================
# Dùng setup_spark() từ config.py — cùng cách với 8 script còn lại.
# setup_spark() đã xử lý 4 vấn đề Windows:
#   1. Trỏ HADOOP_HOME tới winutils.exe (nếu không có, Spark không ghi được Parquet)
#   2. Tạo lệnh python3.exe trong venv (Spark JVM spawn worker bằng tên "python3")
#   3. Đặt PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON đúng phiên bản
#   4. Cố định session.timeZone = UTC — điều quan trọng nhất: nếu không, Spark
#      parse '2019-11-01 00:00:00 UTC' theo giờ máy (GMT+07:00) → lệch 7 tiếng,
#      và các cột day/hour tính theo giờ máy sẽ mâu thuẫn với event_time.
spark = setup_spark("Data_Collect", master="local[*]", shuffle_partitions=8,
                    driver_memory="4g", log_level="WARN")

print("=" * 50)
print("SPARK SESSION ĐÃ KHỞI TẠO")
print("=" * 50)

# Đường dẫn file
DATA_PATH = "Data/raw/2019-Nov.csv"

# Đọc CSV
df = spark.read.csv(NOV_FILE, header=True)

print(f"✅ Đã đọc {df.count():,} records")
print(f"Columns: {df.columns}")
df.printSchema()

# ===============================
# 3. EDA - KHÁM PHÁ DỮ LIỆU
# ===============================

print("\n" + "=" * 50)
print("EDA - KHÁM PHÁ DỮ LIỆU")
print("=" * 50)

# 3.1: Số dòng
print(f"\n3.1 Tổng số records: {df.count():,}")

# 3.2: Schema
print("\n3.2 Schema:")
df.printSchema()

# 3.3: Sample data
print("\n3.3 Sample 5 dòng đầu:")
df.show(5, truncate=False)

# 3.4: Thống kê cơ bản
print("\n3.4 Thống kê numeric columns:")
df.describe().show()

# 3.5: Null values
print("\n3.5 Số lượng NULL trong mỗi cột:")
for col in df.columns:
    null_count = df.filter(df[col].isNull()).count()
    if null_count > 0:
        print(f"   {col}: {null_count:,} NULLs")
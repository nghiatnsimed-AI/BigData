"""
KIỂM TRA CHẤT LƯỢNG TẦNG SILVER
Đọc trực tiếp từ Data/silver/ (không tính lại từ Bronze) nên chạy rất nhanh.

Quy trình kiểm tra:
  1. Số dòng và dung lượng mỗi bảng
  2. Referential integrity: mọi dòng fact phải tìm thấy cha trong dimension
  3. Độ bao phủ dữ liệu (tỉ lệ sự kiện có danh mục / thương hiệu)
  4. Phân bố các cột phân loại và nhãn CDC
  5. Đối chiếu tổng số dòng với tầng Bronze
"""

import os
import time

from config import *
from pyspark.sql import functions as F

spark = setup_spark("Data_Verify", master="local[*]", shuffle_partitions=8,
                    driver_memory="4g", log_level="WARN")

SILVER = DATA_SILVER
BRONZE = DATA_BRONZE
CDC_DIR = os.path.join(BRONZE, "cdc")

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


banner("KIỂM TRA CHẤT LƯỢNG TẦNG SILVER")

# ===============================
# 1. ĐỌC TẦNG SILVER TỪ ĐĨA
# ===============================
log("Đọc tầng Silver từ đĩa...")

fact = spark.read.parquet(os.path.join(SILVER, "fact_event"))
dim_product = spark.read.parquet(os.path.join(SILVER, "dim_product"))
dim_user = spark.read.parquet(os.path.join(SILVER, "dim_user"))
dim_category = spark.read.parquet(os.path.join(SILVER, "dim_category"))

# ===============================
# 2. SỐ DÒNG VÀ DUNG LƯỢNG
# ===============================
log("Đếm số dòng...")

n_fact = fact.count()
n_product = dim_product.count()
n_user = dim_user.count()
n_category = dim_category.count()

print("\n" + "=" * 58)
print("SỐ DÒNG VÀ DUNG LƯỢNG TẦNG SILVER")
print("=" * 58)
for name, n in [("fact_event", n_fact), ("dim_product", n_product),
                ("dim_user", n_user), ("dim_category", n_category)]:
    size, files = dir_size(os.path.join(SILVER, name))
    print(f"  {name:<16}: {n:>12,} dòng  |  {files:>3} file  |  {human_bytes(size)}")

# ===============================
# 3. ĐỐI CHIẾU VỚI TẦNG BRONZE
# ===============================
log("Đối chiếu với tầng Bronze...")

n_bronze = (
    spark.read.parquet(os.path.join(BRONZE, "oct_2019.parquet")).count()
    + spark.read.parquet(os.path.join(BRONZE, "nov_2019.parquet")).count()
)

print("\n" + "=" * 58)
print("ĐỐI CHIẾU BRONZE → SILVER")
print("=" * 58)
print(f"  Tổng sự kiện ở Bronze        : {n_bronze:>12,}")
print(f"  Tổng sự kiện ở Silver fact   : {n_fact:>12,}")
print(f"  Bản ghi trùng lặp đã loại     : {n_bronze - n_fact:>12,}")
print(f"  Tỉ lệ giữ lại                 : {n_fact / n_bronze * 100:>11.4f}%")

# ===============================
# 4. REFERENTIAL INTEGRITY
# ===============================
# Mỗi dòng fact phải tìm thấy "cha" tương ứng trong bảng chiều.
# Nếu có dòng không tìm thấy cha, mô hình sao bị vỡ và mọi phân tích
# phía sau sẽ cho kết quả sai.
log("Kiểm tra khóa ngoại (referential integrity)...")

# Với mỗi cặp (khóa con, khóa cha): đếm số khóa con KHÔNG tìm thấy trong cha.
# Dùng left anti join thay vì left join + filter để tránh kéo theo toàn bộ payload.
checks = [
    ("fact_event → dim_product", "product_id", "product_id", dim_product),
    ("fact_event → dim_user", "user_id", "user_id", dim_user),
    ("fact_event → dim_category", "category_code", "category_code", dim_category),
]

print("\n" + "=" * 58)
print("REFERENTIAL INTEGRITY")
print("=" * 58)
for label, child_col, parent_col, parent_df in checks:
    parent_keys = parent_df.select(parent_col).distinct()
    orphan = (
        fact.select(child_col).distinct()
        .join(parent_keys, child_col, "left_anti")
        .count()
    )
    status = "OK" if orphan == 0 else "LỖI"
    print(f"  {label:<30}: {orphan:>6} khóa mồ côi  [{status}]")

# ===============================
# 5. ĐỘ BAO PHỦ DỮ LIỆU
# ===============================
log("Đo độ bao phủ dữ liệu...")

coverage = fact.agg(
    F.count(F.lit(1)).alias("total"),
    F.sum(F.when(F.col("is_classified"), 1).otherwise(0)).alias("classified"),
    F.sum(F.when(F.col("is_branded"), 1).otherwise(0)).alias("branded"),
    F.sum(F.when(F.col("price") > 0, 1).otherwise(0)).alias("has_price"),
).collect()[0]

print("\n" + "=" * 58)
print("ĐỘ BAO PHỦ DỮ LIỆU (sau khi khử trùng lặp)")
print("=" * 58)
print(f"  Tổng sự kiện                        : {coverage['total']:>12,}")
print(f"  Có danh mục (is_classified)         : {coverage['classified']:>12,} "
      f"({coverage['classified'] / coverage['total'] * 100:.2f}%)")
print(f"  KHÔNG có danh mục                   : {coverage['total'] - coverage['classified']:>12,} "
      f"({(coverage['total'] - coverage['classified']) / coverage['total'] * 100:.2f}%)")
print(f"  Có thương hiệu (is_branded)         : {coverage['branded']:>12,} "
      f"({coverage['branded'] / coverage['total'] * 100:.2f}%)")
print(f"  KHÔNG có thương hiệu                : {coverage['total'] - coverage['branded']:>12,} "
      f"({(coverage['total'] - coverage['branded']) / coverage['total'] * 100:.2f}%)")
print(f"  Có giá > 0                          : {coverage['has_price']:>12,} "
      f"({coverage['has_price'] / coverage['total'] * 100:.2f}%)")

print("\n  → Nhóm 'KHÔNG có danh mục' là sản phẩm thực sự không thuộc danh mục nào")
print("    (Missing Not At Random), được đánh dấu minh bạch bằng cờ is_classified.")
print("    Phân tích theo danh mục phải loại nhóm này khỏi mẫu số.")

# ===============================
# 6. PHÂN BỐ CÁC CỘT PHÂN LOẠI
# ===============================
print("\n" + "=" * 58)
print("PHÂN BỐ CÁC CỘT PHÂN LOẠI")
print("=" * 58)

print("\n  customer_type (theo dim_user):")
dim_user.groupBy("customer_type").count().orderBy("count", ascending=False).show(truncate=False)

print("  change_type (theo dim_product, từ CDC):")
dim_product.groupBy("change_type").count().orderBy("count", ascending=False).show(truncate=False)

print("  user_status (theo dim_user, từ CDC):")
dim_user.groupBy("user_status").count().orderBy("count", ascending=False).show(truncate=False)

print("  is_classified (theo dim_product):")
dim_product.groupBy("is_classified").count().orderBy("count", ascending=False).show(truncate=False)

# ===============================
# 7. KIỂM TRA DẢI THỜI GIAN
# ===============================
log("Kiểm tra dải thời gian...")

# LƯU Ý QUAN TRỌNG về hiển thị múi giờ:
# Spark lưu timestamp trong Parquet dưới dạng số micro-giây kể từ epoch UTC.
# Khi đọc lên, giá trị được hiển thị theo múi giờ CỦA DRIVER PYTHON (GMT+07:00 ở máy này),
# nên 2019-10-01 00:00:00 UTC bị in ra thành 2019-10-01 07:00:00.
# Vì vậy KHÔNG được so sánh trực tiếp chuỗi hiển thị. Cách kiểm tra đúng là so sánh
# các phái sinh (hour, day, event_date) với giá trị tính lại từ event_time trong
# cùng ngữ cảnh UTC của Spark. Nếu khớp 100%, dữ liệu đúng.
ts = fact.agg(
    F.min("event_date").cast("string").alias("min_date"),
    F.max("event_date").cast("string").alias("max_date"),
    F.min(F.to_date("event_time")).cast("string").alias("min_date_check"),
    F.max(F.to_date("event_time")).cast("string").alias("max_date_check"),
).collect()[0]

m_hour = fact.filter(F.hour(F.col("event_time")) != F.col("hour")).count()
m_day = fact.filter(F.dayofmonth(F.col("event_time")) != F.col("day")).count()
m_date = fact.filter(F.to_date(F.col("event_time")) != F.col("event_date")).count()

print("\n" + "=" * 58)
print("DẢI THỜI GIAN VÀ TÍNH NHẤT QUÁN CỘT PHÁI SINH")
print("=" * 58)
print(f"  Khoảng ngày (theo event_date)      : {ts['min_date']} → {ts['max_date']}")
print(f"  Khoảng ngày (tính lại từ event_time): {ts['min_date_check']} → {ts['max_date_check']}")
print(f"  Số dòng lệch cột 'hour'   : {m_hour:,}")
print(f"  Số dòng lệch cột 'day'    : {m_day:,}")
print(f"  Số dòng lệch cột 'event_date': {m_date:,}")

ok = (
    ts["min_date"] == ts["min_date_check"]
    and ts["max_date"] == ts["max_date_check"]
    and m_hour == 0 and m_day == 0 and m_date == 0
    and str(ts["min_date"]).startswith("2019-10-01")
    and str(ts["max_date"]).startswith("2019-11-30")
)
print(f"  → {'OK — dữ liệu nhất quán, không lệch múi giờ' if ok else 'LỖI — cần chạy lại Data_cleaning.py'}")
print("\n  Ghi chú: giá trị in ra có thể lệch 7 tiếng so với nguồn vì Python driver")
print("  hiển thị timestamp theo múi giờ máy. Giá trị thực lưu trong Parquet là UTC.")

print(f"\n⏱  Tổng thời gian kiểm tra: {time.time() - t0:.1f}s")
print("=" * 58)
print("✅ KIỂM TRA TẦNG SILVER HOÀN TẤT!")
print("=" * 58)

spark.stop()

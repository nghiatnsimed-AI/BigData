"""
GIAI ĐOẠN 1.3: LÀM SẠCH DỮ LIỆU
E-commerce Behavior Data Pipeline
"""

import time

from config import *
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, to_timestamp, regexp_replace, split, when,
    count, lit, isnan, year, month, dayofmonth,
    hour, date_format
)

# ===============================
# 1. KHỞI TẠO SPARK
# ===============================
# setup_spark() trong config.py xử lý 3 lỗi Windows:
#   - winutils.exe (permission khi ghi Parquet)
#   - python3.exe (spawn Python Worker)
#   - session.timeZone = UTC (nguồn CSV ghi chuẩn UTC)
spark = setup_spark("Data_Cleaning", master="local[*]", shuffle_partitions=8,
                    driver_memory="4g", log_level="WARN")

t_start = time.time()


def log(msg):
    print(f"[{time.time() - t_start:7.1f}s] {msg}", flush=True)


print("=" * 60)
print("GIAI ĐOẠN 1.3: LÀM SẠCH DỮ LIỆU")
print("=" * 60)

# ===============================
# 2. ĐỌC DỮ LIỆU THÔ
# ===============================
print("\n[1] Đọc dữ liệu từ raw...")

df_nov = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(NOV_FILE)

#inferSchema=true  → Đọc hết data → Tự đoán kiểu → CHẬM với 67 triệu rows
# inferSchema=false → Đọc 1 dòng → Giữ string hết → NHANH
# → Với Big Data, ta tự convert kiểu sau

df_oct = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(OCT_FILE)

log(f"Nov: {df_nov.count():,} records")
log(f"Oct: {df_oct.count():,} records")

# Kiểm tra NULL trước clean
print("\n[1.5] Kiểm tra NULL trước clean...")
for col_name in df_nov.columns:
    null_count = df_nov.filter(df_nov[col_name].isNull()).count()
    if null_count > 0:
        print(f"    {col_name}: {null_count:,} NULLs")

# ===============================
# 3. HÀM LÀM SẠCH DỮ LIỆU
# ===============================
print("\n[2] Làm sạch dữ liệu...")


def clean_data(df, month_name):
    """
    Làm sạch và convert dữ liệu

    Steps:
    1. Convert event_time (xử lý UTC suffix)
    2. Convert kiểu dữ liệu
    3. Xử lý NULL values
    4. Xử lý NaN values
    5. Tạo derived columns
    """

    # --- 3.1: Convert event_time (xử lý " UTC" suffix) ---
    # Input: "2019-11-01 00:00:00 UTC"
    # Output: timestamp 2019-11-01 00:00:00
    # Session timezone đã set = UTC trong setup_spark(), nên giá trị parse
    # được khớp đúng với nhãn " UTC" trong file nguồn.
    df = df.withColumn(
        "event_time",
        to_timestamp(
            regexp_replace(col("event_time"), " UTC$", ""),
            "yyyy-MM-dd HH:mm:ss"
        )
    )

    # --- 3.2: Convert kiểu dữ liệu ---
    df = df.withColumn("product_id", col("product_id").cast("long"))
    df = df.withColumn("category_id", col("category_id").cast("long"))
    df = df.withColumn("price", col("price").cast("double"))
    df = df.withColumn("user_id", col("user_id").cast("long"))

    # --- 3.3: Xử lý NULL values ---
    df = df.fillna({
        "category_code": "unknown.unknown",
        "brand": "unknown",
        "user_session": "unknown"
    })

    # --- 3.4: Xử lý NaN values ---
    # Price NaN → Fill = 0
    df = df.withColumn(
        "price",
        when(isnan(df["price"]), 0.0).otherwise(df["price"])
    )

    # --- 3.5: Tạo derived columns ---
    # Tách ngày/tháng/năm
    df = df.withColumn("year", year(col("event_time")))
    df = df.withColumn("month", month(col("event_time")))
    df = df.withColumn("day", dayofmonth(col("event_time")))
    df = df.withColumn("hour", hour(col("event_time")))
    df = df.withColumn("date", col("event_time").cast("date"))

    # Tách category_code: "electronics.smartphone" → level1, level2, level3
    df = df.withColumn(
        "category_level1",
        split(col("category_code"), "\\.").getItem(0)
    )
    df = df.withColumn(
        "category_level2",
        split(col("category_code"), "\\.").getItem(1)
    )
    df = df.withColumn(
        "category_level3",
        split(col("category_code"), "\\.").getItem(2)
    )

    # Tạo event_date string cho partition
    df = df.withColumn(
        "event_date",
        date_format(col("event_time"), "yyyy-MM-dd")
    )

    print(f"    {month_name}: Clean xong!")
    return df


# Áp dụng clean cho cả 2 datasets
df_nov_clean = clean_data(df_nov, "November")
df_oct_clean = clean_data(df_oct, "October")

# ===============================
# 4. KIỂM TRA SAU CLEAN
# ===============================
print("\n[3] Kiểm tra schema sau clean...")
df_nov_clean.printSchema()

print("\n[4] Sample data sau clean...")
df_nov_clean.select(
    "event_time", "event_type", "product_id", "brand",
    "price", "category_level1", "category_level2"
).show(5, truncate=False)

print("\n[5] Thống kê price sau clean...")
df_nov_clean.select("price").describe().show()

# Kiểm tra NULL sau clean
print("\n[6] Kiểm tra NULL sau clean...")
for col_name in df_nov_clean.columns:
    null_count = df_nov_clean.filter(df_nov_clean[col_name].isNull()).count()
    if null_count > 0:
        print(f"    {col_name}: {null_count:,} NULLs")

# Kiểm tra tính nhất quán múi giờ: min/max event_time phải nằm trong kỳ tương ứng
print("\n[6.5] Kiểm tra dải thời gian (UTC)...")
df_nov_clean.select(
    F.min("event_time").alias("min_t"),
    F.max("event_time").alias("max_t"),
).show(truncate=False)
df_oct_clean.select(
    F.min("event_time").alias("min_t"),
    F.max("event_time").alias("max_t"),
).show(truncate=False)

# ===============================
# 5. LƯU VÀO BRONZE LAYER
# ===============================
print("\n[7] Lưu vào Bronze Layer (Parquet)...")

# Lưu Nov
df_nov_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(os.path.join(DATA_BRONZE, "nov_2019.parquet"))

# Lưu Oct
df_oct_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(os.path.join(DATA_BRONZE, "oct_2019.parquet"))

for name in ["oct_2019.parquet", "nov_2019.parquet"]:
    p = os.path.join(DATA_BRONZE, name)
    size, count = dir_size(p)
    print(f"    ✅ {name}: {count} file, {human_bytes(size)}")

print("\n" + "=" * 60)
print(f"✅ GIAI ĐOẠN 1.3 HOÀN THÀNH! ({time.time() - t_start:.1f}s)")
print("=" * 60)

spark.stop()

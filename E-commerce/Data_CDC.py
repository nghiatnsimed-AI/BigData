"""
GIAI ĐOẠN 1.4: CDC (Change Data Capture)
So sánh hành vi giữa 2 kỳ Oct 2019 và Nov 2019 trên tầng Bronze.

Input : Data/bronze/oct_2019.parquet, Data/bronze/nov_2019.parquet
Output: Data/bronze/cdc/  (Parquet + CSV tổng hợp) + console report
"""

import os
import time

from config import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel

spark = setup_spark("Data_CDC", master="local[4]", shuffle_partitions=8,
                    driver_memory="6g", log_level="WARN")

CDC_OUT = os.path.join(DATA_BRONZE, "cdc")
OCT_BRONZE = os.path.join(DATA_BRONZE, "oct_2019.parquet")
NOV_BRONZE = os.path.join(DATA_BRONZE, "nov_2019.parquet")

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


banner("GIAI ĐOẠN 1.4: CDC - SO SÁNH OCT 2019 vs NOV 2019")

# ===============================
# 1. ĐỌC BRONZE
# ===============================
log("Đọc tầng Bronze...")
df_oct = spark.read.parquet(OCT_BRONZE)
df_nov = spark.read.parquet(NOV_BRONZE)

# Cache để các phép tổng hợp phía sau không phải quét lại 110M rows từ đĩa.
# Đây là điểm tối ưu quan trọng: cùng một dữ liệu được dùng cho nhiều phép
# aggregate (snapshot, period_stats, tổng quan) - không cache thì mỗi phép lại
# đọc toàn bộ Parquet từ đĩa.
#
# LƯU Ý QUAN TRỌNG — phải dùng MEMORY_AND_DISK, không dùng .cache():
# .cache() = StorageLevel.MEMORY_ONLY. Với 110M dòng (~110M x 9 cột) thì
# phần nhớ trong RAM không đủ chứa -> Spark đổ phần dư xuống disk, nhưng
# quá trình ghi vẫn cần giữ block trong heap để serialize -> vượt 4 GB ->
# "java.lang.OutOfMemoryError: Java heap space" ở Stage 6 (17/10/2026).
# MEMORY_AND_DISK: phần vừa RAM giữ trong RAM, phần dư ghi thẳng xuống disk,
# không bao giờ phải chứa toàn bộ block trong heap.
df_oct = df_oct.persist(StorageLevel.MEMORY_AND_DISK)
df_nov = df_nov.persist(StorageLevel.MEMORY_AND_DISK)
df_oct.count()   # trigger cache
df_nov.count()   # trigger cache
log("Đã cache 2 partition (MEMORY_AND_DISK)")

# ===============================
# 2. BẢNG SNAPSHOT SẢN PHẨM
# ===============================
# Mỗi sản phẩm -> 1 dòng tóm tắt: giá, số event, số user, category, brand.
log("Tạo snapshot sản phẩm cho 2 kỳ...")

snapshot = df_oct.select(
    "product_id", "category_code", "brand", "price", "event_type",
    "user_id", "event_time"
).union(df_nov.select(
    "product_id", "category_code", "brand", "price", "event_type",
    "user_id", "event_time"
))

# Dùng approx_count_distinct thay vì countDistinct chính xác:
# - nhanh hơn nhiều trên 110M rows
# - lỗi sai số ~2%, chấp nhận được cho thống kê "số user unique"
product_snapshot = snapshot.groupBy("product_id").agg(
    F.first("category_code", ignorenulls=True).alias("category_code"),
    F.first("brand", ignorenulls=True).alias("brand"),
    F.max("price").alias("max_price"),
    F.min("price").alias("min_price"),
    F.avg("price").alias("avg_price"),
    F.count(F.lit(1)).alias("total_events"),
    F.approx_count_distinct("user_id").alias("unique_users"),
    F.min("event_time").alias("first_seen"),
    F.max("event_time").alias("last_seen"),
)

# ===============================
# 3. SO SÁNH 2 KỲ
# ===============================
log("So sánh 2 kỳ...")


def period_stats(df, period):
    """Tổng hợp theo product_id cho 1 kỳ."""
    return df.groupBy("product_id").agg(
        F.count(F.lit(1)).alias(f"events_{period}"),
        F.approx_count_distinct("user_id").alias(f"users_{period}"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(f"purchases_{period}"),
        F.max("price").alias(f"price_{period}"),
        F.max("event_time").alias(f"last_seen_{period}"),
    )


oct_ps = period_stats(df_oct, "oct")
nov_ps = period_stats(df_nov, "nov")

# Full outer join: giữ sản phẩm chỉ có ở 1 kỳ
cdc = oct_ps.join(nov_ps, on="product_id", how="full_outer")

# Gắn thuộc tính sản phẩm từ snapshot chung
cdc = cdc.join(
    product_snapshot.select("product_id", "category_code", "brand", "avg_price"),
    on="product_id", how="left",
)

# Phân loại thay đổi
cdc = cdc.withColumn(
    "change_type",
    F.when(F.col("events_oct").isNull() & F.col("events_nov").isNotNull(), "NEW_PRODUCT")
     .when(F.col("events_nov").isNull() & F.col("events_oct").isNotNull(), "DISCONTINUED")
     .when(F.col("price_oct") != F.col("price_nov"), "PRICE_CHANGED")
     .otherwise("UNCHANGED"),
)

# Mức độ ưu tiên
cdc = cdc.withColumn(
    "change_severity",
    F.when(F.col("change_type") == "NEW_PRODUCT", F.lit(1))
     .when(F.col("change_type") == "DISCONTINUED", F.lit(2))
     .when(F.col("change_type") == "PRICE_CHANGED", F.lit(3))
     .otherwise(F.lit(4)),
)

cdc = cdc.withColumn(
    "price_delta",
    F.round(F.col("price_nov") - F.col("price_oct"), 2),
)

# ===============================
# 4. TỔNG HỢP THEO CATEGORY
# ===============================
log("Tổng hợp theo category...")

cat_cdc = cdc.groupBy("category_code").agg(
    F.count(F.lit(1)).alias("products_total"),
    F.sum(F.when(F.col("change_type") == "NEW_PRODUCT", 1).otherwise(0)).alias("new_products"),
    F.sum(F.when(F.col("change_type") == "DISCONTINUED", 1).otherwise(0)).alias("discontinued_products"),
    F.sum(F.when(F.col("change_type") == "PRICE_CHANGED", 1).otherwise(0)).alias("price_changed_products"),
    F.round(F.avg("price_delta"), 4).alias("avg_price_delta"),
    F.sum("events_oct").alias("events_oct"),
    F.sum("events_nov").alias("events_nov"),
).withColumn(
    "event_growth_pct",
    F.round(
        (F.col("events_nov") - F.col("events_oct")) / F.col("events_oct") * 100, 2
    ),
)

# ===============================
# 5. USER MỚI / USER RỜI ĐI
# ===============================
log("So sánh tập user giữa 2 kỳ...")

# approx_count_distinct trên user_id: nhanh hơn nhiều lần so với join
# distinct 2 bảng 3.7M + 3.9M dòng, lỗi sai số ~2%.
oct_users = df_oct.select("user_id").distinct()
nov_users = df_nov.select("user_id").distinct()

users = oct_users.withColumn("in_oct", F.lit(1)).join(
    nov_users.withColumn("in_nov", F.lit(1)),
    on="user_id", how="full_outer",
)

user_cdc = users.withColumn(
    "user_status",
    F.when(F.col("in_oct").isNull(), "NEW_USER")
     .when(F.col("in_nov").isNull(), "CHURNED_USER")
     .otherwise("RETAINED"),
)

# ===============================
# 6. GHI KẾT QUẢ
# ===============================
log("Ghi kết quả CDC ra Data/bronze/cdc/...")

os.makedirs(CDC_OUT, exist_ok=True)

cdc.write.mode("overwrite").parquet(os.path.join(CDC_OUT, "product_cdc.parquet"))
cat_cdc.write.mode("overwrite").parquet(os.path.join(CDC_OUT, "category_cdc.parquet"))
user_cdc.write.mode("overwrite").parquet(os.path.join(CDC_OUT, "user_cdc.parquet"))

for df, name in [(cdc, "product_cdc"), (cat_cdc, "category_cdc"), (user_cdc, "user_cdc")]:
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        os.path.join(CDC_OUT, f"{name}_csv")
    )

# ===============================
# 7. BÁO CÁO CONSOLE
# ===============================
banner("BÁO CÁO CDC")

log("Tổng quan 2 kỳ...")
# Dùng API DataFrame thay vì spark.sql() vì biến Python không tự đăng ký
# thành bảng tạm trong catalog. Tính tất cả trong MỘT lần aggregate để chỉ
# quét dữ liệu 1 vòng thay vì 6 vòng.
overview = (
    df_oct.agg(
        F.count(F.lit(1)).alias("oct_events"),
        F.countDistinct("product_id").alias("oct_products"),
        F.countDistinct("user_id").alias("oct_users"),
    )
    .crossJoin(
        df_nov.agg(
            F.count(F.lit(1)).alias("nov_events"),
            F.countDistinct("product_id").alias("nov_products"),
            F.countDistinct("user_id").alias("nov_users"),
        )
    )
    .collect()[0]
)

print(f"  Oct 2019: {overview['oct_events']:>12,} events | "
      f"{overview['oct_products']:>7,} products | {overview['oct_users']:>9,} users")
print(f"  Nov 2019: {overview['nov_events']:>12,} events | "
      f"{overview['nov_products']:>7,} products | {overview['nov_users']:>9,} users")
print(f"  Tăng trưởng events: "
      f"{(overview['nov_events'] - overview['oct_events']) / overview['oct_events'] * 100:.1f}%")

log("Phân bố thay đổi sản phẩm...")
cdc.groupBy("change_type").count().orderBy("count", ascending=False).show(truncate=False)

log("Top 10 sản phẩm mới...")
cdc.filter(F.col("change_type") == "NEW_PRODUCT").orderBy(
    F.col("events_nov").desc()
).select(
    "product_id", "category_code", "brand", "price_nov", "events_nov", "users_nov"
).show(10, truncate=False)

log("Top 10 sản phẩm bị gỡ (không còn trong Nov)...")
cdc.filter(F.col("change_type") == "DISCONTINUED").orderBy(
    F.col("events_oct").desc()
).select(
    "product_id", "category_code", "brand", "price_oct", "events_oct", "users_oct"
).show(10, truncate=False)

log("Top 10 sản phẩm tăng giá mạnh nhất...")
cdc.filter(F.col("change_type") == "PRICE_CHANGED").orderBy(
    F.col("price_delta").desc()
).select(
    "product_id", "category_code", "price_oct", "price_nov", "price_delta"
).show(10, truncate=False)

log("Top 10 category tăng trưởng mạnh nhất...")
cat_cdc.filter(F.col("events_oct") > 0).orderBy(
    F.col("event_growth_pct").desc()
).select(
    "category_code", "events_oct", "events_nov", "event_growth_pct",
    "new_products", "discontinued_products"
).show(10, truncate=False)

log("Phân bố user...")
user_cdc.groupBy("user_status").count().show(truncate=False)

# ===============================
# 8. TỔNG KẾT
# ===============================
banner("KẾT QUẢ CDC")

total_products = cdc.count()
changed = cdc.filter(F.col("change_type") != "UNCHANGED").count()

print(f"  Tổng sản phẩm theo dõi : {total_products:,}")
print(f"  Sản phẩm có thay đổi    : {changed:,} ({changed / total_products * 100:.1f}%)")
print(f"  Sản phẩm mới (Nov)     : {cdc.filter(F.col('change_type') == 'NEW_PRODUCT').count():,}")
print(f"  Sản phẩm bị gỡ         : {cdc.filter(F.col('change_type') == 'DISCONTINUED').count():,}")
print(f"  Sản phẩm đổi giá       : {cdc.filter(F.col('change_type') == 'PRICE_CHANGED').count():,}")
print()
print(f"  User mới trong Nov     : {user_cdc.filter(F.col('user_status') == 'NEW_USER').count():,}")
print(f"  User rời đi            : {user_cdc.filter(F.col('user_status') == 'CHURNED_USER').count():,}")
print(f"  User giữ lại           : {user_cdc.filter(F.col('user_status') == 'RETAINED').count():,}")
print()
print(f"  Output: {CDC_OUT}")
for name in ["product_cdc.parquet", "category_cdc.parquet", "user_cdc.parquet"]:
    p = os.path.join(CDC_OUT, name)
    size, count = dir_size(p)
    print(f"    - {name}: {count} file, {human_bytes(size)}")

print()
print(f"⏱  Tổng thời gian: {time.time() - t0:.1f}s")
print("=" * 60)
print("✅ GIAI ĐOẠN 1.4 HOÀN THÀNH!")
print("=" * 60)

df_oct.unpersist()
df_nov.unpersist()
spark.stop()

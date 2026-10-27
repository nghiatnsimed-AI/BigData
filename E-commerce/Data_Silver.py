"""
GIAI ĐOẠN 2: TẦNG SILVER — TÁCH CHIỀU VÀ KHÔI PHỤC CHẤT LƯỢNG
E-commerce Behavior Data Pipeline

Input : Data/bronze/{oct,nov}_2019.parquet + Data/bronze/cdc/*.parquet
Output: Data/silver/{dim_product, dim_user, dim_category, fact_event}

Nguyên tắc thiết kế:
  - Bronze giữ dữ liệu ở dạng sự kiện (event log), chưa tách chiều.
  - Silver tách thành mô hình sao: 3 bảng chiều + 1 bảng sự kiện.
  - Khôi phục category_code/brand bị thiếu bằng cách suy ra từ
    CÙNG product_id ở các dòng khác (99.93% sản phẩm có dữ liệu nhất quán).
  - Loại bỏ trùng lặp (0.15% bản ghi trùng hoàn toàn).
"""

import os
import time

from config import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = setup_spark("Data_Silver", master="local[*]", shuffle_partitions=8,
                    driver_memory="4g", log_level="WARN")

SILVER = DATA_SILVER
CDC_DIR = os.path.join(DATA_BRONZE, "cdc")

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


banner("GIAI ĐOẠN 2: TẦNG SILVER")

# ===============================
# 1. ĐỌC BRONZE
# ===============================
log("Đọc tầng Bronze...")

# Chỉ đọc các cột cần dùng -> column pruning giúp giảm I/O
EVENT_COLS = [
    "event_time", "event_type", "product_id", "category_id",
    "category_code", "brand", "price", "user_id", "user_session",
    "year", "month", "day", "hour", "date", "event_date",
    "category_level1", "category_level2", "category_level3",
]

df_oct = spark.read.parquet(os.path.join(DATA_BRONZE, "oct_2019.parquet")).select(*EVENT_COLS)
df_nov = spark.read.parquet(os.path.join(DATA_BRONZE, "nov_2019.parquet")).select(*EVENT_COLS)

events = df_oct.unionByName(df_nov)
log(f"Đọc xong: {events.count():,} sự kiện (2 kỳ)")

# ===============================
# 2. PHÂN LOẠI DANH MỤC
# ===============================
# Khảo sát trên CSV gốc cho thấy:
#   - 82.729 sản phẩm (43%) có category_code ở ít nhất một dòng
#   - 107.933 sản phẩm (57%) KHÔNG có category_code ở bất kỳ dòng nào
#   => Sản phẩm nhóm thứ hai thực sự không thuộc danh mục nào.
#      Đây KHÔNG phải dữ liệu thiếu có thể suy ra — mà là thuộc tính của sản phẩm.
#
# Vì vậy thay vì "khôi phục", ta gán nhãn rõ ràng:
#   UNCLASSIFIED  = sản phẩm không có danh mục nào trong toàn bộ lịch sử
#   CLASSIFIED    = sản phẩm có danh mục
# Cột này giúp phân tích BIẾT chính xác danh mục nào bị ảnh hưởng bởi
# nhóm "không phân loại", thay vì giả vờ dữ liệu đầy đủ.
log("Phân loại sản phẩm có/không thuộc danh mục...")

prod_cat = (
    events.filter(F.col("category_code") != "unknown.unknown")
    .groupBy("product_id")
    .agg(
        F.countDistinct("category_code").alias("n_cat"),
        F.first("category_code").alias("cat_value"),
    )
)

# Sản phẩm chỉ dùng mapping khi có ĐÚNG 1 danh mục (không mơ hồ)
prod_cat = prod_cat.filter(F.col("n_cat") == 1).select(
    "product_id", F.col("cat_value").alias("cat_known")
)

prod_brand = (
    events.filter(F.col("brand") != "unknown")
    .groupBy("product_id")
    .agg(
        F.countDistinct("brand").alias("n_brand"),
        F.first("brand").alias("brand_value"),
    )
    .filter(F.col("n_brand") == 1)
    .select(
        "product_id", F.col("brand_value").alias("brand_known")
    )
)

log("Áp dụng nhãn phân loại...")
events = (
    events
    .join(prod_cat, on="product_id", how="left")
    .join(prod_brand, on="product_id", how="left")
    .withColumn(
        "is_classified",
        F.when(F.col("cat_known").isNotNull(), F.lit(True)).otherwise(F.lit(False)),
    )
    .withColumn(
        "is_branded",
        F.when(F.col("brand_known").isNotNull(), F.lit(True)).otherwise(F.lit(False)),
    )
    .drop("cat_known", "brand_known")
)

# Tách lại cấp danh mục (giữ nguyên để nhất quán với Bronze)
events = (
    events
    .withColumn("category_level1", F.split(F.col("category_code"), "\\.").getItem(0))
    .withColumn("category_level2", F.split(F.col("category_code"), "\\.").getItem(1))
    .withColumn("category_level3", F.split(F.col("category_code"), "\\.").getItem(2))
)

# ===============================
# 3. LOẠI BỎ TRÙNG LẮP
# ===============================
# 100.530 dòng (0.15%) trùng hoàn toàn trên khóa sự kiện.
# Giữ lại bản ghi đầu tiên, bỏ qua các bản ghi lặp lại y hệt.
log("Loại bỏ bản ghi trùng lặp...")
before = events.count()
events = events.dropDuplicates(
    ["event_time", "event_type", "product_id", "user_id", "user_session"]
)
after = events.count()
log(f"Trước: {before:,} → Sau: {after:,} (bỏ {before - after:,} dòng trùng)")

# ===============================
# 3b. GHI FACT_EVENT RA ĐĨA TRƯỚC (thay cho persist trong RAM)
# ===============================
# BỐI CẢNH — VÌ SAO KHÔNG DÙNG persist():
# Ban đầu tôi dùng events.persist(StorageLevel.MEMORY_AND_DISK) để tránh việc
# Spark tính lại chuỗi join + dropDuplicates 110 triệu dòng nhiều lần. Nhưng
# Spark cache kiểu columnar cho DataFrame này cần hơn 4 GB heap ->
# OutOfMemoryError: Java heap space, job bị abort sau 229 giây.
#
# GIẢI PHÁP: ghi fact_event ra đĩa MỘT LẦN, rồi đọc lại từ đĩa để dựng chiều.
#   - Chi phí: 1 lần ghi (Bronze + join + dedup) + vài lần quét Parquet đã nén.
#   - Lợi ích: không tốn RAM; đọc Parquet chỉ mất vài giây, nhanh hơn nhiều so với
#     việc Spark tính lại chuỗi join + dropDuplicates từ đầu cho từng bảng dimension.
#   - Lưu ý quan trọng: dữ liệu mà dimension đọc CHÍNH LÀ dữ liệu đã ghi xuống
#     đĩa -> phần kiểm tra chất lượng sau đó là kiểm tra trên dữ liệu thật.
os.makedirs(SILVER, exist_ok=True)
log("Ghi fact_event ra đĩa (thay cho cache trong RAM)...")

fact_event = events.select(
    F.col("event_time"), F.col("date"), F.col("year"), F.col("month"),
    F.col("day"), F.col("hour"), F.col("event_date"), F.col("event_type"),
    F.col("product_id"), F.col("category_code"), F.col("brand"),
    F.col("user_id"), F.col("user_session"),
    F.col("category_level1"), F.col("category_level2"), F.col("category_level3"),
    F.col("price"), F.col("is_classified"), F.col("is_branded"),
)

# GHI RA ĐĨA — dòng này trước đây BỊ THIẾU.
# Bug phát hiện 25/10/2026 khi chạy lại pipeline: code chỉ `select` xong rồi
# `read.parquet()` lại chính thư mục đó, nên `fact_event` trên đĩa là dữ liệu
# CŨ từ 24/10, không phải kết quả của lần chạy hiện tại. Dấu hiệu: file
# Parquet trong Data/silver/fact_event/ giữ nguyên mtime cũ, không đổi sau
# khi script chạy. Hậu quả: nếu Bronze thay đổi thì Silver vẫn dùng số cũ ->
# pipeline không còn tái lập được (không idempotent).
# mode("overwrite") + partitionBy(year, month) xoá partition cũ rồi ghi mới.
fact_event.write.mode("overwrite").partitionBy("year", "month").parquet(
    os.path.join(SILVER, "fact_event")
)

log("fact_event đã ghi xong. Đọc lại từ đĩa để dựng chiều...")

events = spark.read.parquet(os.path.join(SILVER, "fact_event"))
log(f"Đã đọc lại: {events.count():,} sự kiện từ đĩa.")

# ===============================
# 4. TÁCH CHIỀU
# ===============================
# ===============================
# 4.1 dim_category
# ===============================
log("Tạo dim_category...")
dim_category = (
    events.groupBy("category_code")
    .agg(
        F.first("category_level1").alias("level1"),
        F.first("category_level2").alias("level2"),
        F.first("category_level3").alias("level3"),
        F.countDistinct("product_id").alias("product_count"),
        F.count(F.lit(1)).alias("event_count"),
        F.countDistinct("user_id").alias("user_count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.max("price").alias("max_price"),
        F.min("price").alias("min_price"),
    )
    .withColumn("category_code", F.col("category_code"))
)

# ===============================
# 4.2 dim_product
# ===============================
log("Tạo dim_product...")
# Giá: lấy giá phổ biến nhất (mode) thay vì max, tránh nhiễu từ giá 0
price_window = Window.partitionBy("product_id").orderBy(F.col("event_count").desc())
product_price = (
    events.filter(F.col("price") > 0)
    .groupBy("product_id", "price")
    .agg(F.count(F.lit(1)).alias("event_count"))
    .withColumn("rn", F.row_number().over(price_window))
    .filter(F.col("rn") == 1)
    .select("product_id", F.col("price").alias("list_price"))
)

dim_product = (
    events.groupBy("product_id")
    .agg(
        F.first("category_code").alias("category_code"),
        F.first("brand").alias("brand"),
        F.first("category_level1").alias("level1"),
        F.first("category_level2").alias("level2"),
        F.first("is_classified").alias("is_classified"),
        F.first("is_branded").alias("is_branded"),
        F.count(F.lit(1)).alias("total_events"),
        F.approx_count_distinct("user_id").alias("unique_users"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .join(product_price, on="product_id", how="left")
    .withColumn(
        "list_price",
        F.when(F.col("list_price").isNull(), F.col("avg_price")).otherwise(F.col("list_price")),
    )
    .select(
        "product_id", "category_code", "brand", "level1", "level2",
        "is_classified", "is_branded",
        "list_price", "avg_price", "total_events", "unique_users",
        "first_seen", "last_seen",
    )
)

# ===============================
# 4.3 dim_user
# ===============================
log("Tạo dim_user...")
dim_user = (
    events.groupBy("user_id")
    .agg(
        F.approx_count_distinct("user_session").alias("session_count"),
        F.count(F.lit(1)).alias("total_events"),
        F.approx_count_distinct("product_id").alias("products_viewed"),
        F.countDistinct("category_code").alias("categories_viewed"),
        F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("purchase_count"),
        F.sum(F.when(F.col("event_type") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
        F.min("event_time").alias("first_seen"),
        F.max("event_time").alias("last_seen"),
        # Tổng chi CHỈ tính từ sự kiện purchase.
        # Nếu sum(price) trên mọi event thì sẽ tính cả giá của lượt xem và lượt
        # thêm giỏ — người dùng chỉ xem sản phẩm 100.000 USD cũng bị coi là
        # đã chi tiêu 100.000 USD. Đây là lỗi sai lệch mà không ai nhận ra
        # vì con số vẫn "hợp lý" về mặt hình thức.
        F.round(F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)), 2)
        .alias("total_spent"),
    )
    .withColumn(
        "active_days",
        F.datediff(F.col("last_seen").cast("date"), F.col("first_seen").cast("date")) + 1,
    )
    .withColumn(
        "customer_type",
        F.when(F.col("purchase_count") > 0, F.lit("buyer"))
         .when(F.col("cart_count") > 0, F.lit("cart_only"))
         .otherwise(F.lit("viewer")),
    )
)

# ===============================
# 4.4 fact_event
# ===============================
# fact_event đã được ghi ra đĩa ở bước 3b (trước khi dựng chiều) — giữ nguyên bản ghi.
log("fact_event đã ghi ở bước 3b, bỏ qua phần dựng lại.")

# ===============================
# 5. TÍCH HỢP CDC
# ===============================
# Gắn nhãn thay đổi từ tầng Bronze vào dim_product và dim_user.
log("Tích hợp kết quả CDC vào chiều...")

cdc_product = spark.read.parquet(os.path.join(CDC_DIR, "product_cdc.parquet"))
cdc_user = spark.read.parquet(os.path.join(CDC_DIR, "user_cdc.parquet"))

dim_product = dim_product.join(
    cdc_product.select("product_id", "change_type", "price_delta"),
    on="product_id", how="left",
).withColumn(
    "change_type",
    F.when(F.col("change_type").isNull(), F.lit("UNCHANGED")).otherwise(F.col("change_type")),
)

dim_user = dim_user.join(
    cdc_user.select("user_id", "user_status"),
    on="user_id", how="left",
).withColumn(
    "user_status",
    F.when(F.col("user_status").isNull(), F.lit("UNKNOWN")).otherwise(F.col("user_status")),
)

# ===============================
# 6. GHI TẦNG SILVER
# ===============================
log("Ghi tầng Silver...")

os.makedirs(SILVER, exist_ok=True)

dim_product.write.mode("overwrite").parquet(os.path.join(SILVER, "dim_product"))
dim_user.write.mode("overwrite").parquet(os.path.join(SILVER, "dim_user"))
dim_category.write.mode("overwrite").parquet(os.path.join(SILVER, "dim_category"))

log("Ghi xong. Kiểm tra referential integrity...")

# ===============================
# 7. KIỂM TRA CHẤT LƯỢNG
# ===============================
banner("KIỂM TRA CHẤT LƯỢNG TẦNG SILVER")

# Đọc lại từ đĩa thay vì dùng biến trong bộ nhớ.
# Lý do: nếu dùng biến, mỗi phép .count() lại kích hoạt một lần tính toán
# đầy đủ (join + dedup + CDC) — mất thêm hàng chục phút vô ích.
# Đọc file Parquet vừa ghi ra chỉ mất vài giây và kiểm tra đúng thứ đã lưu trên đĩa.
log("Đọc lại tầng Silver từ đĩa để kiểm tra...")
fact_check = spark.read.parquet(os.path.join(SILVER, "fact_event"))
prod_check = spark.read.parquet(os.path.join(SILVER, "dim_product"))
user_check = spark.read.parquet(os.path.join(SILVER, "dim_user"))
cat_check = spark.read.parquet(os.path.join(SILVER, "dim_category"))

# 7.1. Số dòng mỗi bảng
counts = {
    "fact_event": fact_check.count(),
    "dim_product": prod_check.count(),
    "dim_user": user_check.count(),
    "dim_category": cat_check.count(),
}
print("\nSố dòng mỗi bảng Silver:")
for name, n in counts.items():
    print(f"  {name:<16}: {n:,}")

# 7.2. Referential integrity: mọi product trong fact phải có trong dim_product
# Dùng left_anti join: chỉ giữ dòng KHÔNG tìm thấy cha, không kéo theo payload.
log("Kiểm tra khóa ngoại fact_event → dim_product...")
orphan_product = (
    fact_check.select("product_id").distinct()
    .join(prod_check.select("product_id").distinct(), "product_id", "left_anti")
    .count()
)
orphan_user = (
    fact_check.select("user_id").distinct()
    .join(user_check.select("user_id").distinct(), "user_id", "left_anti")
    .count()
)
orphan_category = (
    fact_check.select("category_code").distinct()
    .join(cat_check.select("category_code").distinct(), "category_code", "left_anti")
    .count()
)

print(f"\nKhóa ngoại (số khóa không tìm thấy cha):")
print(f"  fact_event → dim_product  : {orphan_product:,}")
print(f"  fact_event → dim_user     : {orphan_user:,}")
print(f"  fact_event → dim_category : {orphan_category:,}")

# 7.3. Chất lượng dữ liệu sau phân loại
log("Đo tỉ lệ phân loại thành công...")
quality = fact_check.agg(
    F.count(F.lit(1)).alias("total"),
    F.sum(F.when(F.col("is_classified"), 1).otherwise(0)).alias("cat_ok"),
    F.sum(F.when(F.col("is_branded"), 1).otherwise(0)).alias("brand_ok"),
).collect()[0]

print(f"\nPhân loại dữ liệu (2 kỳ, sau loại trùng):")
print(f"  sự kiện có danh mục      : {quality['cat_ok']:,} "
      f"({quality['cat_ok'] / quality['total'] * 100:.2f}%)")
print(f"  sự kiện KHÔNG có danh mục: {quality['total'] - quality['cat_ok']:,} "
      f"({(quality['total'] - quality['cat_ok']) / quality['total'] * 100:.2f}%)")
print(f"  sự kiện có thương hiệu   : {quality['brand_ok']:,} "
      f"({quality['brand_ok'] / quality['total'] * 100:.2f}%)")
print(f"  sự kiện KHÔNG có thương hiệu: {quality['total'] - quality['brand_ok']:,} "
      f"({(quality['total'] - quality['brand_ok']) / quality['total'] * 100:.2f}%)")
print("  → Nhóm không phân loại là sản phẩm thực sự không thuộc danh mục nào,")
print("    được đánh dấu bằng cờ is_classified / is_branded để phân tích chính xác.")

# ===============================
# 8. BÁO CÁO
# ===============================
banner("TÓM TẮT TẦNG SILVER")

print("\nKích thước các bảng trên đĩa:")
for name in ["dim_product", "dim_user", "dim_category", "fact_event"]:
    p = os.path.join(SILVER, name)
    size, count = dir_size(p)
    print(f"  {name:<16}: {count:>3} file, {human_bytes(size)}")

print("\nPhân bố customer_type:")
user_check.groupBy("customer_type").count().orderBy("count", ascending=False).show(truncate=False)

print("Phân bố change_type (sản phẩm):")
prod_check.groupBy("change_type").count().orderBy("count", ascending=False).show(truncate=False)

print("Phân bố user_status:")
user_check.groupBy("user_status").count().orderBy("count", ascending=False).show(truncate=False)

print("\nTop 10 danh mục theo số sản phẩm:")
cat_check.orderBy(F.col("product_count").desc()).select(
    "category_code", "level1", "level2", "product_count", "event_count", "avg_price"
).show(10, truncate=False)

print(f"\n⏱  Tổng thời gian: {time.time() - t0:.1f}s")
print("=" * 60)
print("✅ GIAI ĐOẠN 2 (SILVER) HOÀN THÀNH!")
print("=" * 60)

spark.stop()

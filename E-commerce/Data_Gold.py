"""
GIAI ĐOẠN 3: TẦNG GOLD — DATA MART & KPI TỔNG HỢP
E-commerce Behavior Data Pipeline

Input : Data/silver/fact_event + dim_product + dim_user + dim_category
Output: Data/gold/*.parquet (các bảng KPI sẵn sàng cho Dashboard)

Nguyên tắc thiết kế:
  - Gold là lớp CUỐI CÙNG phục vụ phân tích, không phục vụ xử lý thêm.
  - Mỗi bảng trả lời đúng 1 câu hỏi nghiệp vụ -> truy vấn mili-giây.
  - Tất cả số đo đều có sẵn ở dạng tổng hợp, Dashboard KHÔNG cần quét fact_event.
  - Số liệu doanh thu chỉ tính trên sản phẩm có phân loại (is_classified=True),
    và có báo cáo riêng phần không phân loại để không bị "thất lạc" doanh thu.
"""

import os
import time

from config import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = setup_spark("Data_Gold", master="local[*]", shuffle_partitions=8,
                    driver_memory="4g", log_level="WARN")

GOLD = DATA_GOLD

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


banner("GIAI ĐOẠN 3: TẦNG GOLD - DATA MART & KPI")

# ===============================
# 1. ĐỌC SILVER
# ===============================
log("Đọc tầng Silver...")

fact = spark.read.parquet(os.path.join(DATA_SILVER, "fact_event"))
dim_product = spark.read.parquet(os.path.join(DATA_SILVER, "dim_product"))
dim_category = spark.read.parquet(os.path.join(DATA_SILVER, "dim_category"))

log(f"fact_event: {fact.count():,} dòng")

# ===============================
# 2. CHUẨN BỊ: cờ doanh thu
# ===============================
# Quy ước nghiệp vụ của dataset này:
#   - event_type "purchase" = đơn hàng hoàn tất
#   - price là GIÁ NIÊM YẾT của sản phẩm (mọi event đều mang giá của sản phẩm đó)
#   => Doanh thu ước tính = số lần purchase * price của sản phẩm
#   - price = 0 nghĩa là sản phẩm chưa được gán giá -> KHÔNG tính doanh thu,
#     nhưng VẪN tính số lượt purchase (tồn tại trong fact, chỉ là không có giá trị tiền)
fact = fact.withColumn(
    "is_purchase", F.when(F.col("event_type") == "purchase", 1).otherwise(0)
).withColumn(
    "is_cart", F.when(F.col("event_type") == "cart", 1).otherwise(0)
).withColumn(
    "is_view", F.when(F.col("event_type") == "view", 1).otherwise(0)
).withColumn(
    "has_price", F.when(F.col("price") > 0, 1).otherwise(0)
).withColumn(
    "revenue", F.when(
        (F.col("event_type") == "purchase") & (F.col("price") > 0),
        F.col("price"),
    ).otherwise(F.lit(0.0))
)

# ===============================
# 3. KPI 1 — DOANH THU THEO NGÀY
# ===============================
# Câu hỏi: Doanh thu mỗi ngày thay đổi thế nào, 2 tháng so với nhau?
log("KPI 1: Doanh thu theo ngày...")
kpi_daily_revenue = (
    fact.groupBy(F.to_date("event_time").alias("date"), "year", "month")
    .agg(
        F.sum("revenue").alias("revenue"),
        F.count(F.lit(1)).alias("total_events"),
        F.sum("is_view").alias("views"),
        F.sum("is_cart").alias("carts"),
        F.sum("is_purchase").alias("purchases"),
        F.approx_count_distinct("user_id").alias("active_users"),
        F.approx_count_distinct("product_id").alias("active_products"),
    )
    .withColumn(
        "conversion_rate",
        F.when(F.col("views") > 0, F.round(F.col("purchases") / F.col("views"), 4)).otherwise(0),
    )
    .withColumn("avg_order_value", F.when(
        F.col("purchases") > 0, F.round(F.col("revenue") / F.col("purchases"), 2)
    ).otherwise(0))
    .orderBy("date")
)

# ===============================
# 4. KPI 2 — HIỆU QUẢ THEO DANH MỤC
# ===============================
# Câu hỏi: Danh mục nào bán chạy, doanh thu tăng hay giảm giữa 2 tháng?
log("KPI 2: Hiệu quả theo danh mục...")

cat_month = (
    fact.filter(F.col("is_classified"))
    .groupBy("category_code", "category_level1", "category_level2", "year", "month")
    .agg(
        F.sum("revenue").alias("revenue"),
        F.count(F.lit(1)).alias("total_events"),
        F.sum("is_view").alias("views"),
        F.sum("is_cart").alias("carts"),
        F.sum("is_purchase").alias("purchases"),
        F.approx_count_distinct("user_id").alias("active_users"),
        F.approx_count_distinct("product_id").alias("product_count"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
)

# Pivot để so sánh Oct vs Nov cùng một danh mục
cat_pivot = (
    cat_month.groupBy("category_code", "category_level1", "category_level2")
    .pivot("month", [10, 11])
    .agg(
        F.sum("revenue").alias("revenue"),
        F.sum("purchases").alias("purchases"),
        F.sum("views").alias("views"),
        F.sum("total_events").alias("events"),
    )
)

kpi_category_performance = (
    cat_pivot
    # Tên cột sau pivot có dạng "<tháng>_<chỉ số>", ví dụ "10_revenue", "11_purchases".
    .withColumn("revenue_oct", F.coalesce(F.col("10_revenue"), F.lit(0.0)))
    .withColumn("revenue_nov", F.coalesce(F.col("11_revenue"), F.lit(0.0)))
    .withColumn("purchases_oct", F.coalesce(F.col("10_purchases"), F.lit(0)))
    .withColumn("purchases_nov", F.coalesce(F.col("11_purchases"), F.lit(0)))
    .withColumn("views_oct", F.coalesce(F.col("10_views"), F.lit(0)))
    .withColumn("views_nov", F.coalesce(F.col("11_views"), F.lit(0)))
    .withColumn("events_oct", F.coalesce(F.col("10_events"), F.lit(0)))
    .withColumn("events_nov", F.coalesce(F.col("11_events"), F.lit(0)))
    .withColumn(
        "revenue_growth_pct",
        F.when(F.col("revenue_oct") > 0,
               F.round((F.col("revenue_nov") - F.col("revenue_oct")) / F.col("revenue_oct") * 100, 2))
        .otherwise(F.lit(None)),
    )
    .withColumn(
        "event_growth_pct",
        F.when(F.col("events_oct") > 0,
               F.round((F.col("events_nov") - F.col("events_oct")) / F.col("events_oct") * 100, 2))
        .otherwise(F.lit(None)),
    )
    .withColumn(
        "conversion_rate_nov",
        F.when(F.col("views_nov") > 0, F.round(F.col("purchases_nov") / F.col("views_nov"), 4))
        .otherwise(0),
    )
    .withColumn("total_revenue", F.col("revenue_oct") + F.col("revenue_nov"))
    .drop("10_revenue", "11_revenue", "10_purchases", "11_purchases",
          "10_views", "11_views", "10_events", "11_events")
)

# ===============================
# 5. KPI 3 — PHỄU CHUYỂN ĐỔI (FUNNEL)
# ===============================
# Câu hỏi: Từ view -> cart -> purchase, người dùng rớt ở bước nào?
log("KPI 3: Phễu chuyển đổi...")

funnel_base = fact.groupBy("year", "month").agg(
    F.sum("is_view").alias("views"),
    F.sum("is_cart").alias("carts"),
    F.sum("is_purchase").alias("purchases"),
    F.approx_count_distinct("user_id").alias("users"),
)

kpi_conversion_funnel = (
    funnel_base
    .withColumn("view_to_cart_rate",
                F.when(F.col("views") > 0, F.round(F.col("carts") / F.col("views"), 4)).otherwise(0))
    .withColumn("cart_to_purchase_rate",
                F.when(F.col("carts") > 0, F.round(F.col("purchases") / F.col("carts"), 4)).otherwise(0))
    .withColumn("overall_conversion_rate",
                F.when(F.col("views") > 0, F.round(F.col("purchases") / F.col("views"), 4)).otherwise(0))
    .withColumn("view_to_cart_drop_pct",
                F.when(F.col("views") > 0, F.round((1 - F.col("carts") / F.col("views")) * 100, 2)).otherwise(0))
    .withColumn("cart_to_purchase_drop_pct",
                F.when(F.col("carts") > 0, F.round((1 - F.col("purchases") / F.col("carts")) * 100, 2)).otherwise(0))
)

# Funnel theo danh mục cấp 1 (dùng để vẽ biểu đồ so sánh)
kpi_funnel_by_category = (
    fact.groupBy("year", "month", "category_level1")
    .agg(
        F.sum("is_view").alias("views"),
        F.sum("is_cart").alias("carts"),
        F.sum("is_purchase").alias("purchases"),
        F.sum("revenue").alias("revenue"),
    )
    .withColumn("view_to_cart_rate",
                F.when(F.col("views") > 0, F.round(F.col("carts") / F.col("views"), 4)).otherwise(0))
    .withColumn("overall_conversion_rate",
                F.when(F.col("views") > 0, F.round(F.col("purchases") / F.col("views"), 4)).otherwise(0))
)

# ===============================
# 6. KPI 4 — LƯU LƯỢNG THEO GIỜ
# ===============================
# Câu hỏi: Giờ nào cao điểm? Giờ nào bán hàng tốt nhất?
log("KPI 4: Lưu lượng theo giờ...")
kpi_hourly_traffic = (
    fact.groupBy("hour", "year", "month")
    .agg(
        F.count(F.lit(1)).alias("total_events"),
        F.sum("is_view").alias("views"),
        F.sum("is_cart").alias("carts"),
        F.sum("is_purchase").alias("purchases"),
        F.sum("revenue").alias("revenue"),
        F.approx_count_distinct("user_id").alias("active_users"),
    )
    .withColumn("conversion_rate",
                F.when(F.col("views") > 0, F.round(F.col("purchases") / F.col("views"), 4)).otherwise(0))
    .withColumn("events_per_hour", F.round(F.col("total_events") / F.col("active_users"), 2))
)

# ===============================
# 7. KPI 5 — XẾP HẠNG SẢN PHẨM
# ===============================
# Câu hỏi: Sản phẩm nào bán chạy nhất, sản phẩm nào "được xem nhiều nhưng không ai mua"?
log("KPI 5: Xếp hạng sản phẩm...")

product_agg = fact.groupBy("product_id", "category_code", "brand").agg(
    F.sum("revenue").alias("revenue"),
    F.sum("is_view").alias("views"),
    F.sum("is_cart").alias("carts"),
    F.sum("is_purchase").alias("purchases"),
    F.count(F.lit(1)).alias("total_events"),
    F.approx_count_distinct("user_id").alias("unique_users"),
    F.round(F.avg("price"), 2).alias("avg_price"),
)

kpi_product_ranking = (
    product_agg
    .withColumn("conversion_rate",
                F.when(F.col("views") > 0, F.round(F.col("purchases") / F.col("views"), 4)).otherwise(0))
    .withColumn("view_to_purchase_gap", F.col("views") - F.col("purchases"))
    .withColumn(
        "rank_by_revenue",
        F.rank().over(Window.orderBy(F.col("revenue").desc())),
    )
    .withColumn(
        "rank_by_views",
        F.rank().over(Window.orderBy(F.col("views").desc())),
    )
    .withColumn(
        "rank_by_conversion",
        F.rank().over(Window.orderBy(F.col("conversion_rate").desc())),
    )
)

# ===============================
# 8. KPI 6 — PHÂN KHÚC NGƯỜI DÙNG (RFM)
# ===============================
# Câu hỏi: Khách hàng nào trung thành, ai sắp rời đi, ai mới?
log("KPI 6: Phân khúc người dùng RFM...")

dim_user = spark.read.parquet(os.path.join(DATA_SILVER, "dim_user"))

# Recency = số ngày từ lần cuối user hoạt động, tính so với ngày hoạt động
# cuối cùng của TOÀN BỘ hệ thống (không dùng ngày hiện tại vì dữ liệu là 2019).
last_active = dim_user.agg(F.max("last_seen").alias("max_last_seen")).collect()[0]["max_last_seen"]

rfm = dim_user.filter(F.col("customer_type") == "buyer").select(
    "user_id", "total_events", "total_spent", "purchase_count", "last_seen"
).withColumn(
    "recency_days",
    F.datediff(F.lit(last_active).cast("date"), F.col("last_seen").cast("date")),
)

# Tính ngưỡng phân vị MỘT LẦN rồi dùng lại.
# VÌ SAO KHÔNG DÙNG percentile_approx(...).over(Window.partitionBy()):
# percentile_approx cần một tập dữ liệu đầy đủ để tính, nhưng khi đặt trong
# window function thì Spark tính nó trên TỪNG partition của window — mà window
# ở đây là toàn bộ tập, nên kết quả trả về một giá trị duy nhất cho mọi dòng.
# Hậu quả: mọi người dùng rơi vào cùng một nhóm (chỉ có "VIP" duy nhất).
# Cách đúng: tính ngưỡng ở dạng scalar, rồi dùng CASE WHEN để chấm điểm.
thresholds = rfm.agg(
    *[F.percentile_approx(c, q).alias(f"{c}_{q}")
      for c in ("recency_days", "purchase_count", "total_spent")
      for q in (0.2, 0.4, 0.6, 0.8)]
).collect()[0]


def score_by_percentile(col, higher_is_better=True):
    """
    Chấm điểm 1-5 theo phân vị 20/40/60/80 (ngưỡng đã tính sẵn ở `thresholds`).

    THAM SỐ `higher_is_better` quyết định CHIỀU của thang điểm:
      - higher_is_better=True  : giá trị CÀNG LỚN càng tốt  (dùng cho
        Frequency = số lần mua, Monetary = tổng chi tiêu)
      - higher_is_better=False : giá trị CÀNG NHỎ càng tốt  (dùng cho
        Recency = số ngày từ lần cuối hoạt động — càng ít ngày càng gần)

    LƯU Ý (lỗi đã sửa): trước đây tham số mang tên `ascending` nhưng hai nhánh
    bên trong bị ĐẢO NGƯỢC so với tên gọi — nhánh `ascending=True` lại chấm
    "nhỏ = 5 điểm" và nhánh còn lại chấm "lớn = 5 điểm". Hệ quả: nhóm VIP là
    nhóm mua ít, chi ít (avg_spent 94,7 USD) — hoàn toàn ngược nghĩa.
    Bài học: đặt tên tham số theo NGỮ NGHĨA ("giá trị lớn hơn có tốt hơn không")
    thay vì theo hướng sắp xếp, vì hướng sắp xếp dễ bị đảo khi copy code.
    """
    t = thresholds
    if higher_is_better:
        # giá trị lớn = tốt -> 5 điểm
        return (
            F.when(F.col(col) >= t[f"{col}_0.8"], F.lit(5))
             .when(F.col(col) >= t[f"{col}_0.6"], F.lit(4))
             .when(F.col(col) >= t[f"{col}_0.4"], F.lit(3))
             .when(F.col(col) >= t[f"{col}_0.2"], F.lit(2))
             .otherwise(F.lit(1))
        )
    # giá trị nhỏ = tốt -> 5 điểm
    return (
        F.when(F.col(col) <= t[f"{col}_0.2"], F.lit(5))
         .when(F.col(col) <= t[f"{col}_0.4"], F.lit(4))
         .when(F.col(col) <= t[f"{col}_0.6"], F.lit(3))
         .when(F.col(col) <= t[f"{col}_0.8"], F.lit(2))
         .otherwise(F.lit(1))
    )


kpi_user_segment = (
    rfm
    .withColumn("r_score", score_by_percentile("recency_days", higher_is_better=False))
    .withColumn("f_score", score_by_percentile("purchase_count", higher_is_better=True))
    .withColumn("m_score", score_by_percentile("total_spent", higher_is_better=True))
    .withColumn(
        "rfm_score",
        F.concat(F.col("r_score").cast("string"),
                 F.col("f_score").cast("string"),
                 F.col("m_score").cast("string")),
    )
    .withColumn(
        "segment",
        F.when((F.col("r_score") >= 4) & (F.col("f_score") >= 4) & (F.col("m_score") >= 4), F.lit("VIP"))
         .when((F.col("r_score") >= 4) & (F.col("f_score") >= 3), F.lit("Loyal"))
         .when((F.col("r_score") <= 2) & (F.col("f_score") >= 3), F.lit("At Risk"))
         .when((F.col("r_score") >= 4) & (F.col("f_score") <= 2), F.lit("New"))
         .when(F.col("m_score") >= 4, F.lit("Big Spender"))
         .otherwise(F.lit("Regular")),
    )
)

# ===============================
# 9. KPI 7 — ĐỐI CHIẾU DOANH THU THEO PHÂN LOẠI
# ===============================
# Câu hỏi: Bao nhiêu doanh thu đến từ sản phẩm KHÔNG phân loại?
# Đây là bảng "minh bạch" - chứng minh không bỏ sót doanh thu nào.
log("KPI 7: Đối chiếu doanh thu theo phân loại...")
kpi_revenue_reconciliation = (
    fact.groupBy("year", "month")
    .agg(
        F.sum(F.when(F.col("is_classified"), F.col("revenue")).otherwise(0)).alias("revenue_classified"),
        F.sum(F.when(~F.col("is_classified"), F.col("revenue")).otherwise(0)).alias("revenue_unclassified"),
        F.sum("revenue").alias("revenue_total"),
        F.sum(F.when(F.col("is_classified"), F.col("is_purchase")).otherwise(0)).alias("purchases_classified"),
        F.sum(F.when(~F.col("is_classified"), F.col("is_purchase")).otherwise(0)).alias("purchases_unclassified"),
    )
    .withColumn(
        "unclassified_pct",
        F.when(F.col("revenue_total") > 0,
               F.round(F.col("revenue_unclassified") / F.col("revenue_total") * 100, 2)).otherwise(0),
    )
)

# ===============================
# 10. GHI TẦNG GOLD
# ===============================
log("Ghi tầng Gold...")

os.makedirs(GOLD, exist_ok=True)

TABLES = {
    "kpi_daily_revenue": kpi_daily_revenue,
    "kpi_category_performance": kpi_category_performance,
    "kpi_conversion_funnel": kpi_conversion_funnel,
    "kpi_funnel_by_category": kpi_funnel_by_category,
    "kpi_hourly_traffic": kpi_hourly_traffic,
    "kpi_product_ranking": kpi_product_ranking,
    "kpi_user_segment": kpi_user_segment,
    "kpi_revenue_reconciliation": kpi_revenue_reconciliation,
}

for name, df in TABLES.items():
    df.write.mode("overwrite").parquet(os.path.join(GOLD, name))
    log(f"  ✓ {name}")

# ===============================
# 11. BÁO CÁO
# ===============================
banner("TÓM TẮT TẦNG GOLD")

print("\nKích thước các bảng Gold:")
for name in TABLES:
    p = os.path.join(GOLD, name)
    size, count = dir_size(p)
    print(f"  {name:<30}: {count:>3} file, {human_bytes(size)}")

print("\n=== Doanh thu theo ngày (30 ngày gần nhất) ===")
kpi_daily_revenue.orderBy(F.col("date").desc()).select(
    "date", "revenue", "purchases", "views", "active_users", "conversion_rate"
).show(10, truncate=False)

print("\n=== Top 10 danh mục theo doanh thu ===")
kpi_category_performance.orderBy(F.col("total_revenue").desc()).select(
    "category_code", "category_level1", "revenue_oct", "revenue_nov",
    "revenue_growth_pct", "purchases_nov", "conversion_rate_nov"
).show(10, truncate=False)

print("\n=== Phễu chuyển đổi theo tháng ===")
kpi_conversion_funnel.select(
    "year", "month", "views", "carts", "purchases",
    "view_to_cart_rate", "cart_to_purchase_rate", "overall_conversion_rate"
).show(truncate=False)

print("\n=== Giờ cao điểm (Nov 2019) ===")
kpi_hourly_traffic.filter(F.col("month") == 11).orderBy(
    F.col("total_events").desc()
).select("hour", "total_events", "views", "purchases", "revenue", "active_users").show(8, truncate=False)

print("\n=== Top 10 sản phẩm theo doanh thu ===")
kpi_product_ranking.orderBy(F.col("rank_by_revenue")).select(
    "product_id", "brand", "revenue", "purchases", "views",
    "conversion_rate", "rank_by_revenue"
).show(10, truncate=False)

print("\n=== Phân khúc người dùng ===")
kpi_user_segment.groupBy("segment").agg(
    F.count(F.lit(1)).alias("users"),
    F.round(F.avg("total_spent"), 2).alias("avg_spent"),
    F.round(F.avg("purchase_count"), 2).alias("avg_purchases"),
).orderBy("users", ascending=False).show(truncate=False)

print("\n=== Đối chiếu doanh thu (minh bạch) ===")
kpi_revenue_reconciliation.select(
    "year", "month", "revenue_total", "revenue_classified",
    "revenue_unclassified", "unclassified_pct"
).show(truncate=False)

print(f"\n⏱  Tổng thời gian: {time.time() - t0:.1f}s")
print("=" * 60)
print("✅ GIAI ĐOẠN 3 (GOLD) HOÀN THÀNH!")
print("=" * 60)

spark.stop()

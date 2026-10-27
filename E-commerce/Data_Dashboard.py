"""
GIAI ĐOẠN 4: DASHBOARD & BÁO CÁO TRỰC QUAN
E-commerce Behavior Data Pipeline

Input : Data/gold/*.parquet
Output: Data/dashboard/*.png + Data/dashboard/summary.csv

Vì sao dùng matplotlib thay vì Power BI:
  - Đề tài cho phép "công cụ khác" miễn đúng yêu cầu.
  - Matplotlib chạy offline, tái lập được, không cần cài đặt phần mềm nặng.
  - Xuất PNG kích thước lớn để chèn vào báo cáo .docx.
  - Bảng KPI vẫn xuất ra CSV/Parquet để Power BI có thể đọc trực tiếp nếu cần.
"""

import os
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from config import *
from pyspark.sql import functions as F

spark = setup_spark("Data_Dashboard", master="local[*]", shuffle_partitions=4,
                    driver_memory="3g", log_level="WARN")

DASH = os.path.join(os.path.dirname(DATA_GOLD), "dashboard")
os.makedirs(DASH, exist_ok=True)

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:6.1f}s] {msg}", flush=True)


def save(fig, name):
    path = os.path.join(DASH, name)
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    log(f"  ✓ {name}")


def fix_horiz_axis(ax):
    """Sửa nhãn trục của biểu đồ thanh ngang (barh).

    Với matplotlib 3.11, nhãn trục y của `barh` bị đặt nằm NGANG ở góc trên trái
    (do mặc định rotation=0, va='bottom') → đè lên nhãn tick đầu tiên.
    Ở đây: xoay nhãn 90° (đọc dọc như trục y thông thường) và canh giữa.
    """
    lbl = ax.yaxis.get_label()
    lbl.set_rotation(90)
    lbl.set_va("center")
    lbl.set_ha("center")
    ax.yaxis.set_label_position("left")


banner("GIAI ĐOẠN 4: DASHBOARD")

# Chuẩn bị style chung cho tất cả biểu đồ
plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 12,
    "axes.titleweight": "bold",
    "axes.labelsize": 10,
    "axes.grid": True,
    "grid.alpha": 0.25,
    "grid.linestyle": "--",
    "figure.facecolor": "white",
    "axes.spines.top": False,
    "axes.spines.right": False,
})

INK = "#1f2933"
BLUE = "#2b6cb0"
TEAL = "#2c7a7b"
AMBER = "#b7791f"
RED = "#c53030"
GREY = "#a0aec0"

# ===============================
# 1. ĐỌC GOLD
# ===============================
log("Đọc tầng Gold...")

daily = spark.read.parquet(os.path.join(DATA_GOLD, "kpi_daily_revenue")).toPandas()
cat = spark.read.parquet(os.path.join(DATA_GOLD, "kpi_category_performance")).toPandas()
funnel = spark.read.parquet(os.path.join(DATA_GOLD, "kpi_conversion_funnel")).toPandas()
hourly = spark.read.parquet(os.path.join(DATA_GOLD, "kpi_hourly_traffic")).toPandas()
prod = spark.read.parquet(os.path.join(DATA_GOLD, "kpi_product_ranking")).toPandas()
seg = spark.read.parquet(os.path.join(DATA_GOLD, "kpi_user_segment")).toPandas()
recon = spark.read.parquet(os.path.join(DATA_GOLD, "kpi_revenue_reconciliation")).toPandas()

log(f"daily={len(daily)} cat={len(cat)} hourly={len(hourly)} prod={len(prod)} seg={len(seg)}")

# ===============================
# 2. BIỂU ĐỒ 1 — DOANH THU & SỰ KIỆN THEO NGÀY
# ===============================
log("Biểu đồ 1: Doanh thu theo ngày...")

daily["date"] = pd.to_datetime(daily["date"])
daily = daily.sort_values("date")

fig, ax1 = plt.subplots(figsize=(13, 5.5))
ax2 = ax1.twinx()

ax1.bar(daily["date"], daily["revenue"], width=0.72, color=BLUE, alpha=0.85, label="Doanh thu (USD)")
ax1.set_xlabel("Ngày")
ax1.set_ylabel("Doanh thu (USD)", color=BLUE)
ax1.tick_params(axis="y", labelcolor=BLUE)

ax2.plot(daily["date"], daily["total_events"], color=AMBER, linewidth=2, marker="o",
         markersize=3.5, label="Tổng sự kiện")
ax2.set_ylabel("Tổng sự kiện", color=AMBER)
ax2.tick_params(axis="y", labelcolor=AMBER)
ax2.grid(False)

ax1.set_title("Doanh thu và tổng sự kiện theo ngày (tháng 10 và 11 năm 2019)")
fig.autofmt_xdate(rotation=45)
h1, l1 = ax1.get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
# Đặt chú giải BÊN NGOÀI vùng dữ liệu (trên trục y phải): đặt "upper left" sẽ đè
# lên đường xu hướng tổng sự kiện.
ax1.legend(h1 + h2, l1 + l2, loc="upper left", bbox_to_anchor=(1.005, 1.0),
           frameon=False)
fig.tight_layout()
save(fig, "01_doanh_thu_theo_ngay.png")

# ===============================
# 3. BIỂU ĐỒ 2 — PHỄU CHUYỂN ĐỔI
# ===============================
log("Biểu đồ 2: Phễu chuyển đổi...")

fig, axes = plt.subplots(1, 2, figsize=(13, 5))

for ax, (_, row) in zip(axes, funnel.iterrows()):
    label = "Tháng 10/2019" if row["month"] == 10 else "Tháng 11/2019"
    stages = ["Xem sản phẩm", "Thêm vào giỏ", "Mua hàng"]
    values = [row["views"], row["carts"], row["purchases"]]
    colors = [BLUE, TEAL, AMBER]
    bars = ax.bar(stages, values, color=colors, width=0.6)
    for b, v in zip(bars, values):
        ax.text(b.get_x() + b.get_width() / 2, v, f"{int(v):,}", ha="center",
                va="bottom", fontsize=10, fontweight="bold", color=INK)
    ax.set_title(f"{label} · tỉ lệ chuyển đổi {row['overall_conversion_rate'] * 100:.2f}%")
    ax.set_ylabel("Số sự kiện")
    ax.set_ylim(0, max(values) * 1.15)

fig.suptitle("Phễu chuyển đổi: xem → thêm giỏ → mua hàng", fontsize=13, fontweight="bold")
fig.tight_layout()
save(fig, "02_phieu_chuyen_doi.png")

# ===============================
# 4. BIỂU ĐỒ 3 — TOP DANH MỤC + TĂNG TRƯỞNG
# ===============================
log("Biểu đồ 3: Top danh mục...")

cat = cat.sort_values("total_revenue", ascending=False)
top = cat.head(12).iloc[::-1]

fig, axes = plt.subplots(1, 2, figsize=(14, 6))

bars = axes[0].barh(top["category_code"], top["revenue_nov"], color=BLUE, height=0.65)
for b, v in zip(bars, top["revenue_nov"]):
    axes[0].text(b.get_width(), b.get_y() + b.get_height() / 2, f" {v:,.0f}",
                 va="center", fontsize=8.5, color=INK)
axes[0].set_title("Top 12 danh mục theo doanh thu — tháng 11/2019")
axes[0].set_xlabel("Doanh thu (USD)")
axes[0].set_ylabel("Mã danh mục")
fix_horiz_axis(axes[0])

growth = cat[cat["revenue_oct"] > 0].copy()
growth = growth.sort_values("revenue_growth_pct", ascending=False).head(12).iloc[::-1]
gcolors = [TEAL if g >= 0 else RED for g in growth["revenue_growth_pct"]]
bars = axes[1].barh(growth["category_code"], growth["revenue_growth_pct"], color=gcolors, height=0.65)
for b, v in zip(bars, growth["revenue_growth_pct"]):
    axes[1].text(b.get_width(), b.get_y() + b.get_height() / 2, f" {v:+.0f}%",
                 va="center", fontsize=8.5, color=INK)
axes[1].axvline(0, color=INK, linewidth=0.8)
axes[1].set_title("Danh mục tăng trưởng mạnh nhất (tháng 11 so với tháng 10)")
axes[1].set_xlabel("Tăng trưởng doanh thu (%)")
axes[1].set_ylabel("Mã danh mục")
fix_horiz_axis(axes[1])

fig.suptitle("Hiệu quả kinh doanh theo danh mục", fontsize=13, fontweight="bold")
fig.tight_layout()
save(fig, "03_danh_muc.png")

# ===============================
# 5. BIỂU ĐỒ 4 — LƯU LƯỢNG THEO GIỜ
# ===============================
log("Biểu đồ 4: Lưu lượng theo giờ...")

h_nov = hourly[hourly["month"] == 11].sort_values("hour")
h_oct = hourly[hourly["month"] == 10].sort_values("hour")

fig, axes = plt.subplots(1, 2, figsize=(14, 5))

axes[0].plot(h_nov["hour"], h_nov["total_events"], color=BLUE, linewidth=2,
             marker="o", markersize=4, label="Tháng 11/2019")
axes[0].plot(h_oct["hour"], h_oct["total_events"], color=GREY, linewidth=2,
             linestyle="--", marker="s", markersize=3.5, label="Tháng 10/2019")
axes[0].set_title("Tổng sự kiện theo giờ trong ngày")
axes[0].set_xlabel("Giờ (0-23, theo UTC)")
axes[0].set_ylabel("Tổng sự kiện")
axes[0].set_xticks(range(0, 24, 2))
axes[0].legend(frameon=False)

axes[1].bar(h_nov["hour"], h_nov["conversion_rate"] * 100, color=TEAL, width=0.75)
axes[1].set_title("Tỉ lệ chuyển đổi (mua / xem) theo giờ — tháng 11/2019")
axes[1].set_xlabel("Giờ (0-23, theo UTC)")
axes[1].set_ylabel("Tỉ lệ chuyển đổi (%)")

fig.suptitle("Phân tích theo giờ trong ngày", fontsize=13, fontweight="bold")
fig.tight_layout()
save(fig, "04_luu_luong_theo_gio.png")

# ===============================
# 6. BIỂU ĐỒ 5 — PHÂN KHÚC NGƯỜI DÙNG
# ===============================
log("Biểu đồ 5: Phân khúc người dùng...")

seg_summary = seg.groupby("segment").agg(
    users=("user_id", "count"),
    avg_spent=("total_spent", "mean"),
    avg_purchases=("purchase_count", "mean"),
).reset_index().sort_values("users", ascending=False)

fig, axes = plt.subplots(1, 2, figsize=(13, 5))

seg_order = seg_summary.sort_values("users", ascending=False)
colors = [BLUE, TEAL, AMBER, GREY, RED, "#6b46c1", "#2c7a7b"][:len(seg_order)]
axes[0].bar(seg_order["segment"], seg_order["users"], color=colors, width=0.6)
for i, v in enumerate(seg_order["users"]):
    axes[0].text(i, v, f"{v:,}", ha="center", va="bottom", fontsize=9, fontweight="bold", color=INK)
axes[0].set_title("Số người dùng theo phân khúc RFM")
axes[0].set_ylabel("Số người dùng")
axes[0].tick_params(axis="x", rotation=20)

axes[1].scatter(seg["recency_days"], seg["total_spent"], s=3, alpha=0.25, color=BLUE)
axes[1].set_xscale("symlog")
axes[1].set_yscale("symlog")
axes[1].set_title("Mối quan hệ giữa thời gian không hoạt động và chi tiêu")
axes[1].set_xlabel("Số ngày từ lần cuối hoạt động (thang log)")
axes[1].set_ylabel("Tổng chi tiêu (USD, thang log)")

fig.suptitle("Phân khúc người dùng theo mô hình RFM", fontsize=13, fontweight="bold")
fig.tight_layout()
save(fig, "05_phan_khung_nguoi_dung.png")

# ===============================
# 7. BIỂU ĐỒ 6 — TOP SẢN PHẨM
# ===============================
log("Biểu đồ 6: Top sản phẩm...")

top_prod = prod[prod["revenue"] > 0].nsmallest(12, "rank_by_revenue")
# Sắp xếp TĂNG dần rồi đảo thứ tự để lớn nhất nằm trên cùng.
# KHÔNG dùng ax.invert_yaxis(): hàm này khiến matplotlib chuyển trục x xuống
# TRÊN, trục x chồng lên tiêu đề biểu đồ.
top_prod = top_prod.sort_values("revenue", ascending=True)

fig, ax = plt.subplots(figsize=(12, 5.5))
bars = ax.barh(
    [f"P{p['product_id']}" for _, p in top_prod.iterrows()],
    top_prod["revenue"], color=BLUE, height=0.65,
)
for b, v in zip(bars, top_prod["revenue"]):
    ax.text(b.get_width(), b.get_y() + b.get_height() / 2, f" {v:,.0f}",
            va="center", fontsize=8.5, color=INK)
ax.set_title("Top 12 sản phẩm theo doanh thu (tháng 10 và 11 năm 2019)")
ax.set_xlabel("Doanh thu (USD)")
ax.set_ylabel("Mã sản phẩm")
fix_horiz_axis(ax)
ax.margins(x=0.14)
fig.tight_layout()
save(fig, "06_top_san_pham.png")

# ===============================
# 8. BIỂU ĐỒ 7 — ĐỐI CHIẾU DOANH THU
# ===============================
log("Biểu đồ 7: Đối chiếu doanh thu...")

fig, ax = plt.subplots(figsize=(11, 4.5))
months = ["Tháng 10/2019", "Tháng 11/2019"]
classified = recon["revenue_classified"].values
unclassified = recon["revenue_unclassified"].values
x = np.arange(len(months))

ax.bar(x, classified, width=0.5, color=BLUE, label="Sản phẩm có danh mục")
ax.bar(x, unclassified, width=0.5, bottom=classified, color=AMBER,
       label="Sản phẩm không thuộc danh mục nào")
for i, (c, u) in enumerate(zip(classified, unclassified)):
    total = c + u
    ax.text(i, total, f" {total:,.0f}\n({u / total * 100:.1f}% không phân loại)",
            ha="center", va="bottom", fontsize=9, color=INK)
ax.set_xticks(x)
ax.set_xticklabels(months)
ax.set_ylabel("Doanh thu (USD)")
ax.set_title("Đối chiếu doanh thu theo tình trạng phân loại sản phẩm")
ax.legend(frameon=False)
fig.tight_layout()
save(fig, "07_doi_chieu_doanh_thu.png")

# ===============================
# 9. XUẤT BẢNG KPI TỔNG HỢP
# ===============================
log("Xuất bảng KPI tổng hợp...")

summary = []

tot_rev = float(daily["revenue"].sum())
tot_events = int(daily["total_events"].sum())
tot_purch = int(daily["purchases"].sum())
tot_views = int(daily["views"].sum())

summary.append(["Tổng số sự kiện (2 kỳ)", f"{tot_events:,}", "Đã loại trùng lặp ở tầng Silver"])
summary.append(["Tổng số lượt xem sản phẩm", f"{tot_views:,}", "event_type = view"])
summary.append(["Tổng số lượt mua hàng", f"{tot_purch:,}", "event_type = purchase"])
summary.append(["Tỉ lệ chuyển đổi tổng thể", f"{tot_purch / tot_views * 100:.3f}%", "mua / xem"])
summary.append(["Tổng doanh thu ước tính (USD)", f"{tot_rev:,.0f}", "purchase × giá niêm yết, bỏ sản phẩm giá 0"])
summary.append(["Giá trị đơn hàng trung bình (USD)", f"{tot_rev / tot_purch:,.2f}", "doanh thu / số lượt mua"])
summary.append(["Doanh thu từ sản phẩm không phân loại",
                 f"{recon['revenue_unclassified'].sum():,.0f}",
                 f"{recon['unclassified_pct'].mean():.1f}% tổng doanh thu"])
summary.append(["Số sản phẩm theo dõi", f"{prod['product_id'].count():,}", "distinct product_id"])
summary.append(["Số người dùng có mua hàng", f"{len(seg):,}", "customer_type = buyer"])

df_summary = pd.DataFrame(summary, columns=["Chỉ số", "Giá trị", "Ghi chú"])
df_summary.to_csv(os.path.join(DASH, "tong_hop_kpi.csv"), index=False, encoding="utf-8-sig")

# Xuất các bảng KPI chính ra CSV để Power BI / Excel đọc được
# Liệt kê đủ 8 bảng Gold — trước đó thiếu kpi_funnel_by_category.
for name in ["kpi_daily_revenue", "kpi_category_performance", "kpi_conversion_funnel",
             "kpi_funnel_by_category", "kpi_hourly_traffic", "kpi_product_ranking",
             "kpi_user_segment", "kpi_revenue_reconciliation"]:
    df = spark.read.parquet(os.path.join(DATA_GOLD, name)).toPandas()
    df.to_csv(os.path.join(DASH, f"{name}.csv"), index=False, encoding="utf-8-sig")

print("\n" + "=" * 60)
print("TÓM TẮT KPI")
print("=" * 60)
print(df_summary.to_string(index=False))

print(f"\n⏱  Tổng thời gian: {time.time() - t0:.1f}s")
print(f"📁 Biểu đồ lưu tại: {DASH}")
print("=" * 60)
print("✅ GIAI ĐOẠN 4 (DASHBOARD) HOÀN THÀNH!")
print("=" * 60)

spark.stop()

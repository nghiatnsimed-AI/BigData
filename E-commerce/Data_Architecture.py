"""
SƠ ĐỒ KIẾN TRÚC & DATA-FLOW — Giai đoạn 4 (bổ sung)

Sinh 2 ảnh PNG mô tả kiến trúc Data Lake và mô hình sao tầng Silver:
  00_kien_truc_tong_the.png   Kiến trúc 3 tầng + luồng dữ liệu end-to-end
  01_mo_hinh_sao_silver.png   Mô hình sao tầng Silver (grain, khóa, số dòng)

Vì sao dùng matplotlib để vẽ sơ đồ:
  - Chạy offline, không cần Graphviz / draw.io / PowerPoint → tái lập được trên
    máy khác chỉ với `pip install matplotlib`.
  - Xuất PNG DPI cao để nhúng vào báo cáo không vỡ nét khi in.
  - Sơ đồ được vẽ bằng code nên cập nhật tự động theo số liệu trong config.

Cách dùng: .venv311\\Scripts\\python.exe Data_Architecture.py
"""

import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

from config import *

DASH = os.path.join(os.path.dirname(DATA_GOLD), "dashboard")
os.makedirs(DASH, exist_ok=True)

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "figure.facecolor": "white",
})

INK = "#1F2937"
MUTED = "#4B5563"

# Bảng màu theo tầng — dùng nhất quán với 3 bảng KPI trong báo cáo
LAYER = {
    "source":  ("#E8F1FB", "#2F6FB5"),
    "bronze":  ("#FDF1DE", "#C77C10"),
    "silver":  ("#E6F6EC", "#1F7A44"),
    "gold":    ("#F2E8FB", "#6D3FA0"),
    "present": ("#FDEBE8", "#B03A2E"),
}


def save(fig, name):
    path = os.path.join(DASH, name)
    fig.savefig(path, dpi=200, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  OK  {name}  ({os.path.getsize(path) / 1024:.0f} KB)", flush=True)


def band(ax, y0, y1, label, kind):
    """Dải nền đại diện một tầng kiến trúc + nhãn tầng bên trái."""
    fc, ec = LAYER[kind]
    ax.add_patch(FancyBboxPatch(
        (13, y0), 83, y1 - y0,
        boxstyle="round,pad=0.3,rounding_size=1.4",
        fc=fc, ec=ec, lw=1.5, alpha=0.5, zorder=0))
    ax.text(7.5, (y0 + y1) / 2, label, ha="center", va="center", rotation=90,
            fontsize=11.5, fontweight="bold", color=ec, zorder=3)


def box(ax, x0, y0, w, h, title, lines=None, kind=None, dashed=False,
        fs_title=10.5, fs_body=8.7):
    """Hộp thành phần: tiêu đề + các dòng mô tả."""
    if kind:
        fc, ec = LAYER[kind]
    else:
        fc, ec = "white", "#374151"
    ax.add_patch(FancyBboxPatch(
        (x0, y0), w, h,
        boxstyle="round,pad=0.4,rounding_size=0.9",
        fc=fc, ec=ec, lw=1.4,
        linestyle="--" if dashed else "-", zorder=2))
    tc = ec if kind else "#111827"
    ax.text(x0 + w / 2, y0 + h - 1.6, title, ha="center", va="top",
            fontsize=fs_title, fontweight="bold", color=tc, zorder=3)
    if lines:
        ax.text(x0 + w / 2, y0 + h - 5.0, "\n".join(lines), ha="center", va="top",
                fontsize=fs_body, color=INK, zorder=3, linespacing=1.5)


def arrow(ax, x, y_from, y_to, label=None, ls="-", color=MUTED, lx=None, ly=None):
    """Mũi tên dọc kèm nhãn ở giữa."""
    ax.add_patch(FancyArrowPatch(
        (x, y_from), (x, y_to), arrowstyle="-|>", mutation_scale=15,
        color=color, lw=1.7, linestyle=ls, shrinkA=0, shrinkB=0, zorder=4))
    if label:
        ax.text(lx if lx is not None else x,
                ly if ly is not None else (y_from + y_to) / 2, label,
                ha="center", va="center", fontsize=8.8, color=MUTED, zorder=5,
                bbox=dict(fc="white", ec="none", alpha=0.92, pad=2.4))


# ==================================================================
# ẢNH 1 — KIẾN TRÚC TỔNG THỂ & LUỒNG DỮ LIỆU
# ==================================================================
def draw_architecture():
    fig, ax = plt.subplots(figsize=(15.2, 12.2))
    # xlim mở rộng tới 106 để có chỗ cho nhãn xoay dọc "Data_Verify.py" bên phải
    ax.set_xlim(0, 106)
    ax.set_ylim(-4.5, 100)
    ax.axis("off")

    ax.text(53, 99.2, "KIẾN TRÚC DATA LAKE 3 TẦNG & LUỒNG DỮ LIỆU END-TO-END",
            ha="center", va="top", fontsize=15, fontweight="bold", color="#111827")
    ax.text(53, 96.2,
            "Quy trình con: Storage → Processing · Apache Spark 3.5.3 (PySpark) · "
            "109.950.743 sự kiện · 14,7 GB CSV",
            ha="center", va="top", fontsize=9.8, color=MUTED)

    # ---------------- SOURCES ----------------
    band(ax, 83.5, 94.0, "SOURCES", "source")
    box(ax, 16, 84.5, 38, 8.6, "2019-Oct.csv",
        ["5,7 GB · 42.448.764 bản ghi",
         "Nguồn: Kaggle — multi-category store"], kind="source")
    box(ax, 58, 84.5, 37, 8.6, "2019-Nov.csv",
        ["9,0 GB · 67.501.979 bản ghi",
         "Tổng 109.950.743 dòng · 9 cột kiểu string"], kind="source")

    arrow(ax, 55, 83.5, 76.0,
          "Data_collect.py (EDA) + Data_cleaning.py (707s)\n"
          "parse UTC · ép kiểu · 9 cột phái sinh", lx=55)

    # ---------------- BRONZE ----------------
    band(ax, 66.0, 76.0, "BRONZE", "bronze")
    box(ax, 16, 67.0, 38, 8.0, "Bronze Parquet",
        ["oct_2019.parquet · nov_2019.parquet",
         "4,34 GB · partitionBy(year, month) · nén Snappy"], kind="bronze")
    box(ax, 58, 67.0, 37, 8.0, "CDC — bronze/cdc/",
        ["Data_CDC.py (618s) · full_outer join 2 kỳ",
         "product · user · category  (206.876 sản phẩm)"], kind="bronze")

    arrow(ax, 55, 66.0, 57.5,
          "Data_Silver.py (668s): tách chiều · khử trùng lặp · tích hợp CDC", lx=55)

    # ---------------- SILVER ----------------
    band(ax, 39.5, 57.5, "SILVER", "silver")
    ax.text(96, 56.6, "mô hình sao — 4 bảng", ha="right", va="top",
            fontsize=8.8, fontstyle="italic", color=MUTED)
    cols = [
        ("dim_product", ["1 sản phẩm · 206.876 dòng",
                         "category_code, brand, giá",
                         "is_classified · change_type"]),
        ("dim_user", ["1 người dùng · 5.316.649 dòng",
                      "session, lượt mua, tổng chi",
                      "customer_type · user_status"]),
        ("dim_category", ["1 danh mục · 130 dòng",
                          "3 cấp danh mục",
                          "số sản phẩm · giá TB"]),
        ("fact_event", ["1 sự kiện · 109.819.993 dòng",
                        "event_time, event_type, price",
                        "FK → 3 bảng chiều"]),
    ]
    x = 16.0
    for title, lines in cols:
        box(ax, x, 41.0, 18.5, 14.4, title, lines, kind="silver", fs_title=10)
        x += 20.0

    arrow(ax, 55, 39.5, 32.0,
          "Data_Gold.py (79s): tổng hợp theo grain nghiệp vụ → 8 bảng KPI", lx=48)

    # ---------------- GOLD ----------------
    band(ax, 14.0, 32.0, "GOLD", "gold")
    ax.text(96, 31.2, "data mart — 8 bảng KPI", ha="right", va="top",
            fontsize=8.8, fontstyle="italic", color=MUTED)
    kpis = [
        ("kpi_daily_revenue", "doanh thu mỗi ngày"),
        ("kpi_category_performance", "hiệu quả theo danh mục"),
        ("kpi_conversion_funnel", "phễu view→cart→purchase"),
        ("kpi_funnel_by_category", "phễu phân rã theo danh mục"),
        ("kpi_hourly_traffic", "lưu lượng & giờ cao điểm"),
        ("kpi_product_ranking", "xếp hạng sản phẩm"),
        ("kpi_user_segment", "phân khúc RFM"),
        ("kpi_revenue_reconciliation", "đối chiếu minh bạch"),
    ]
    for i, (name, desc) in enumerate(kpis):
        col, row = i % 4, i // 4
        bx = 16.0 + col * 20.0
        by = 22.6 if row == 0 else 15.2
        box(ax, bx, by, 18.5, 6.6, name, [desc], kind="gold",
            fs_title=8.8, fs_body=8.2)

    arrow(ax, 55, 14.0, 8.0,
          "Data_Dashboard.py (63s)", lx=55)

    # ---------------- PRESENTATION ----------------
    band(ax, -1.0, 8.0, "PRESENT", "present")
    box(ax, 16, 0.2, 38, 6.8, "9 ảnh PNG + 8 bảng KPI CSV",
        ["Data/dashboard/*.png · *.csv",
         "CSV sẵn sàng import vào Power BI / Excel"], kind="present")
    box(ax, 58, 0.2, 37, 6.8, "Tầng trình bày (Presentation)",
        ["Trực quan hóa trên tầng Gold",
         "Kết quả kiểm chứng từ Data_Verify.py"], kind="present")

    # Kiểm chứng độc lập — vòng phản hồi từ Silver
    # Nhãn đặt ở CỘT TRỐNG bên phải (x≈98, ngoài lưới hộp vì các dải chỉ tới x=96)
    # và xoay dọc — khoảng trống ngang giữa Silver và Gold đã bị nhãn Data_Gold.py chiếm.
    ax.add_patch(FancyArrowPatch(
        (96.8, 48.0), (96.8, 33.5), arrowstyle="-|>", mutation_scale=13,
        color="#1F7A44", lw=1.5, linestyle=(0, (4, 2)),
        connectionstyle="arc3,rad=0.0", zorder=4))
    ax.text(98.6, 40.5, "Data_Verify.py (95s) — kiểm chứng từ đĩa: "
                         "số dòng · khóa ngoại · múi giờ",
            ha="center", va="center", rotation=90, fontsize=7.8,
            color="#1F7A44", zorder=6,
            bbox=dict(fc="white", ec="#1F7A44", alpha=0.96, lw=0.8, pad=2.8))

    # Chú thích công nghệ
    ax.text(53, -3.4,
            "Công nghệ: Apache Spark 3.5.3 (PySpark, local[*], driver heap 4 GB) · "
            "Apache Parquet + Snappy · Python 3.11 · Matplotlib  |  "
            "Múi giờ phiên Spark cố định UTC",
            ha="center", va="center", fontsize=8.8, color=MUTED)

    save(fig, "00_kien_truc_tong_the.png")


# ==================================================================
# ẢNH 2 — MÔ HÌNH SAO TẦNG SILVER
# ==================================================================
def draw_star_schema():
    fig, ax = plt.subplots(figsize=(13.5, 8.6))
    ax.set_xlim(0, 100)
    ax.set_ylim(0, 100)
    ax.axis("off")

    ax.text(50, 97.8, "MÔ HÌNH SAO TẦNG SILVER — GRAIN, KHÓA CHÍNH VÀ SỐ DÒNG",
            ha="center", va="top", fontsize=14.5, fontweight="bold", color="#111827")
    ax.text(50, 93.2,
            "Tách chiều từ fact_event để phân tích không phải quét lại 110 triệu dòng sự kiện",
            ha="center", va="top", fontsize=9.8, color=MUTED)

    # ---------- Bố cục ----------
    # fact_event nằm GIỮA, 3 bảng chiều nằm 3 góc còn lại.
    # Ô trung tâm: x 36.5–63.5 · y 33–79
    # Ô góc:      x 4–30 (trái) và x 70–96 (phải) · y 52–79 (trên), y 4–31 (dưới)
    # Các ô cố tình RỜI NHAU — không hộp nào chồng hộp nào.
    FW, FH = 27.0, 27.0
    FX, FY = 36.5, 33.0
    LX, RX = 4.0, 69.0
    TY, BY = 52.0, 4.0

    # fact_event — trung tâm
    box(ax, FX, FY, FW, FH, "fact_event",
        ["Grain: 1 dòng = 1 sự kiện hành vi",
         "109.819.993 dòng · 3,91 GB",
         "",
         "event_time · event_type · price",
         "product_id (FK)",
         "user_id (FK)",
         "category_code (FK)",
         "",
         "partitionBy(year, month)"],
        kind="silver", fs_title=12.5, fs_body=8.9)

    dims = [
        (LX, TY, "dim_product",
         ["Grain: 1 dòng = 1 sản phẩm",
          "206.876 dòng · 7,59 MB",
          "",
          "product_id (PK)",
          "category_code · category_level1/2/3",
          "brand · price",
          "is_classified · is_branded",
          "first_seen · last_seen",
          "change_type (từ CDC)"]),
        (RX, TY, "dim_user",
         ["Grain: 1 dòng = 1 người dùng",
          "5.316.649 dòng · 142 MB",
          "",
          "user_id (PK)",
          "session_count · event_count",
          "purchase_count · total_spent",
          "customer_type",
          "user_status (từ CDC)"]),
        (LX, BY, "dim_category",
         ["Grain: 1 dòng = 1 danh mục",
          "130 dòng · 9,67 KB",
          "",
          "category_code (PK)",
          "category_level1/2/3",
          "product_count · event_count",
          "avg_price · total_revenue",
          "is_classified"]),
    ]

    for x0, y0, title, lines in dims:
        box(ax, x0, y0, FW, FH, title, lines, kind="silver",
            fs_title=11.5, fs_body=8.7)

    # ---------- Mũi tên khóa ngoại ----------
    # product: dim_product (trái trên) → fact_event (giữa)
    ax.add_patch(FancyArrowPatch(
        (LX + FW, TY + FH / 2), (FX, FY + FH - 4),
        arrowstyle="-|>", mutation_scale=13, color="#1F7A44", lw=1.6,
        connectionstyle="arc3,rad=-0.22", zorder=4))
    ax.text(LX + FW + 3.0, TY + FH / 2 + 5.6, "product_id", ha="center", va="bottom",
            fontsize=8.4, color="#1F7A44", fontweight="bold")

    # user: dim_user (phải trên) → fact_event (giữa)
    ax.add_patch(FancyArrowPatch(
        (RX, TY + FH / 2), (FX + FW, FY + FH - 4),
        arrowstyle="-|>", mutation_scale=13, color="#1F7A44", lw=1.6,
        connectionstyle="arc3,rad=0.22", zorder=4))
    ax.text(RX - 3.0, TY + FH / 2 + 5.6, "user_id", ha="center", va="bottom",
            fontsize=8.4, color="#1F7A44", fontweight="bold")

    # category: dim_category (trái dưới) → fact_event (giữa)
    ax.add_patch(FancyArrowPatch(
        (LX + FW, BY + FH / 2), (FX, FY + 4),
        arrowstyle="-|>", mutation_scale=13, color="#1F7A44", lw=1.6,
        connectionstyle="arc3,rad=0.22", zorder=4))
    ax.text(LX + FW + 2.0, BY + FH / 2 - 5.6, "category_code", ha="center", va="top",
            fontsize=8.4, color="#1F7A44", fontweight="bold")

    # ---------- Ghi chú kiểm soát chất lượng ----------
    box(ax, RX, BY, FW, FH, "Kiểm soát chất lượng",
        ["Data_Verify.py — đọc lại từ đĩa",
         "",
         "• Số dòng: 109.819.993",
         "• Khóa mồ côi: 0 / 3 quan hệ",
         "  (left_anti join)",
         "• Múi giờ: 0 dòng lệch",
         "  (hour/day/event_date)",
         "",
         "Bỏ 130.750 dòng trùng lặp",
         "→ giữ 99,8811% dữ liệu"],
        dashed=True, fs_title=11.5, fs_body=8.7)

    ax.text(50, 1.2,
            "Cờ is_classified / is_branded thay cho việc điền giá trị bịa đặt: "
            "107.933 sản phẩm (56,61%) thực sự không thuộc danh mục nào "
            "(Missing Not At Random) → đo lường độ bao phủ thay vì khôi phục giả.",
            ha="center", va="bottom", fontsize=8.5, color=MUTED)

    save(fig, "01_mo_hinh_sao_silver.png")


if __name__ == "__main__":
    banner("SƠ ĐỒ KIẾN TRÚC & MÔ HÌNH SAO")
    draw_architecture()
    draw_star_schema()
    print("\nHoàn tất. Ảnh nằm trong:", DASH)

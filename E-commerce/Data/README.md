# Data — những gì Git giữ và những gì chỉ có trên máy

GitHub **không** chứa CSV gốc hay Parquet. Clone repo xong phải tải raw và chạy pipeline.

## Phải tự chuẩn bị

1. Tải `2019-Oct.csv` và `2019-Nov.csv` vào `raw/`
   (Kaggle: ecommerce-behavior-data-from-multi-category-store).
2. Tạo `.venv311` theo `../README.md`.
3. Chạy lần lượt: `Data_collect.py` → `Data_cleaning.py` → `Data_CDC.py` →
   `Data_Silver.py` → `Data_Verify.py` → `Data_Gold.py` → `Data_Dashboard.py`.

## Đã commit

- `dashboard/*.png` — biểu đồ và sơ đồ kiến trúc
- `dashboard/kpi_*.csv` nhỏ (`daily_revenue`, `category_performance`, funnel, hourly, reconciliation, `tong_hop_kpi`)
- `logs/*_run.log` — log lần chạy thành công

## Cố ý không commit

- `raw/`, `bronze/`, `silver/`, `gold/` — quá nặng
- `dashboard/kpi_user_segment.csv` (~42 MB) và `kpi_product_ranking.csv` (~19 MB)
  — Spark sinh lại khi chạy `Data_Dashboard.py`

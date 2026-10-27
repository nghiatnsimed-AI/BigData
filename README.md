# BigData — đồ án xử lý dữ liệu lớn

Repo học phần **MTDL522 / xử lý dữ liệu lớn**. Phần triển khai chính nằm trong
[`E-commerce/`](E-commerce/).

## Không đẩy lên Git

Dữ liệu pipeline **không** nằm trên GitHub (vượt giới hạn 100 MB/file, tổng ~23 GB):

| Thư mục | Dung lượng máy local | Cách có lại |
|---|---|---|
| `E-commerce/Data/raw/` | ~14 GB CSV | Tải từ [Kaggle](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) |
| `E-commerce/Data/bronze/` | ~4,5 GB | Chạy `Data_cleaning.py` + `Data_CDC.py` |
| `E-commerce/Data/silver/` | ~4,1 GB | Chạy `Data_Silver.py` |
| `E-commerce/Data/gold/` | ~20 MB Parquet | Chạy `Data_Gold.py` |
| `E-commerce/.venv311/` | ~650 MB | `python -m venv .venv311` rồi `pip install -r requirements.txt` |

Chi tiết chạy pipeline: [`E-commerce/README.md`](E-commerce/README.md).

## Có trên Git

Mã nguồn Spark, tài liệu, báo cáo Word, log chạy, biểu đồ PNG, và các CSV KPI nhỏ
(tổng hợp, không phải bảng hàng triệu dòng).

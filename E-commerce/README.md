# E-commerce Behavior Data Pipeline

Đồ án học phần **Phân tích dữ liệu lớn**. Quy trình con được chọn: **Storage → Processing**,
xây dựng Data Lake 3 tầng (Raw → Bronze → Silver → Gold) với Apache Spark / PySpark.

## 1. Yêu cầu môi trường

Làm **đúng thứ tự A → E** trên **Windows**. Spark 3.5.3 chạy trên JVM nên thiếu Java
hoặc `winutils` thì script Python vẫn mở được, nhưng **không ghi được Parquet**.

| Thành phần | Phiên bản | Vì sao bắt buộc |
|---|---|---|
| Java (JDK) | **17** | Spark 3.5.3 chạy trên JVM. JDK 8 quá cũ; JDK 21+ dễ lệch API. |
| Python | **3.11** (không dùng 3.12+) | PySpark 3.5.3 cần module `distutils`. Python 3.12 đã xóa → worker crash. |
| PySpark + pandas + pyarrow + matplotlib | xem `requirements.txt` | Spark, đọc Parquet, vẽ dashboard |
| `winutils.exe` + `hadoop.dll` | Hadoop 3.3.x, Windows 64-bit | Windows không có `chmod`. Thiếu file này → Bronze rỗng. |
| RAM / đĩa | ≥ 16 GB RAM, **~25 GB trống** | Raw ~14 GB + Bronze/Silver ~9 GB + venv ~0,6 GB |

Đường dẫn máy này (khớp `config.py`):

| Biến / file | Đường dẫn cố định |
|---|---|
| `JAVA_HOME` | `D:\Metadata\jdk-17.0.12` |
| `HADOOP_HOME` | `D:\Metadata\hadoop` |
| `winutils.exe` | `D:\Metadata\hadoop\bin\winutils.exe` |
| Venv | `E-commerce\.venv311\` |

Nếu đặt JDK/Hadoop chỗ khác: sửa `JAVA_HOME` trên Windows **và** sửa `hadoop_home`
trong `config.py` (dòng `hadoop_home = r"D:\Metadata\hadoop"`).

---

### 1.1 Cài Java 17 (JDK)

**Bước 1 — Tải.** Vào <https://adoptium.net/temurin/releases/?version=17>
(Eclipse Temurin / OpenJDK). Chọn:

- OS: **Windows**
- Arch: **x64**
- Package: **JDK**
- Nên lấy file **`.zip`** (giải nén tay, dễ đặt đúng `D:\Metadata\...`)
  hoặc **`.msi`** (trình cài có thể tự ghi `JAVA_HOME`).

**Bước 2 — Giải nén (nếu dùng ZIP).** Tạo thư mục `D:\Metadata\` rồi giải nén
sao cho tồn tại file:

```
D:\Metadata\jdk-17.0.12\bin\java.exe
```

Tên thư mục có thể là `jdk-17.0.12+7` tùy bản — đổi tên cho ngắn, hoặc dùng đúng
tên thư mục thật khi gán `JAVA_HOME`. **Không** trỏ `JAVA_HOME` vào `bin`.

**Bước 3 — Biến môi trường (giao diện Windows).**

1. `Win` → gõ **Environment Variables** → *Edit the system environment variables*.
2. *Environment Variables…*
3. Ở **User variables** hoặc **System variables**:
   - **New…** → Name = `JAVA_HOME` · Value = `D:\Metadata\jdk-17.0.12`
     (đúng thư mục có `bin\java.exe`, không thêm `\bin`).
   - Sửa `Path` → **New** → `%JAVA_HOME%\bin`
4. OK hết. **Đóng mọi cửa sổ PowerShell / Cursor / VS Code rồi mở lại**
   (biến môi trường chỉ có hiệu lực ở tiến trình mới).

**Bước 4 — Kiểm tra.** Mở **PowerShell mới**:

```powershell
java -version
echo $env:JAVA_HOME
```

Kết quả đúng dạng:

```
openjdk version "17.0.12" 2024-07-16
OpenJDK Runtime Environment Temurin-17.0.12+7 (...)
OpenJDK 64-Bit Server VM (...)
```

`JAVA_HOME` in ra `D:\Metadata\jdk-17.0.12`.

| Hiện tượng | Cách xử lý |
|---|---|
| `java` không phải lệnh | Chưa thêm `%JAVA_HOME%\bin` vào `Path`, hoặc chưa mở lại terminal |
| In ra Java 8 / 11 / 21 | Máy có nhiều JDK. Đưa `%JAVA_HOME%\bin` **lên đầu** `Path` |
| `JAVA_HOME` trống | Tạo biến đúng tên `JAVA_HOME`, không viết nhầm `JAVA_HONE` |

---

### 1.2 Cài `winutils.exe` + `hadoop.dll` (chỉ Windows)

Spark ghi Parquet qua Hadoop FileSystem. Trên Linux có `chmod`; Windows thì Hadoop
gọi `winutils.exe`. **Thiếu → job chạy xong phần tính toán nhưng không ghi file.**

**Bước 1 — Tải.** Lấy bản **Hadoop 3.3.x, Windows 64-bit**. Nguồn thường dùng:
<https://github.com/cdarlint/winutils> → thư mục `hadoop-3.3.5/bin/`
(hoặc 3.3.6). Cần tối thiểu hai file:

- `winutils.exe`
- `hadoop.dll`

**Bước 2 — Đặt đúng chỗ** (khớp `config.py`):

```
D:\Metadata\hadoop\bin\winutils.exe
D:\Metadata\hadoop\bin\hadoop.dll
```

**Bước 3 — Copy `hadoop.dll` vào JDK** (JVM load native lib từ đây):

```powershell
Copy-Item D:\Metadata\hadoop\bin\hadoop.dll $env:JAVA_HOME\bin\hadoop.dll -Force
```

**Bước 4 — `HADOOP_HOME`.** Tạo biến môi trường:

- Name = `HADOOP_HOME`
- Value = `D:\Metadata\hadoop`  ← **không** thêm `\bin`

`config.py` cũng tự gán `HADOOP_HOME` khi thấy `winutils.exe`, nhưng đặt biến
hệ thống giúp các công cụ khác (Spark UI, lệnh ngoài Python) không báo unset.

**Bước 5 — Kiểm tra:**

```powershell
Test-Path D:\Metadata\hadoop\bin\winutils.exe
Test-Path D:\Metadata\hadoop\bin\hadoop.dll
Test-Path "$env:JAVA_HOME\bin\hadoop.dll"
echo $env:HADOOP_HOME
```

Cả ba `Test-Path` phải là `True`. `HADOOP_HOME` = `D:\Metadata\hadoop`.

---

### 1.3 Python 3.11 + venv + thư viện

**Không dùng Python 3.12/3.13** cho dự án này.

**Bước 1 — Cài Python 3.11.** Tải <https://www.python.org/downloads/release/python-3119/>
bản Windows installer 64-bit. Khi cài:

- Tick **Add python.exe to PATH**
- Giữ *Install launcher*

Kiểm tra (PowerShell mới):

```powershell
py -3.11 --version
```

Phải in `Python 3.11.x`. Nếu `py -3.11` không có, gỡ Python 3.12 khỏi PATH
hoặc cài lại 3.11.

**Bước 2 — Tạo venv trong thư mục dự án:**

```powershell
cd C:\Users\Admin\Documents\CaoHoc\BigData\E-commerce

py -3.11 -m venv .venv311
.\.venv311\Scripts\Activate.ps1
python --version
```

`python --version` phải là 3.11.x. Nếu PowerShell chặn script:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

rồi `Activate.ps1` lại.

**Bước 3 — Cài thư viện:**

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
pip show pyspark
```

`pyspark` phải là **3.5.3**.

**Bước 4 — Tạo `python3.exe`.** Spark (JVM) spawn worker bằng tên `python3`.
Windows `ProcessBuilder` **không** chạy `python3.bat` nếu không có shell.

```powershell
Copy-Item .\.venv311\Scripts\python.exe .\.venv311\Scripts\python3.exe
Test-Path .\.venv311\Scripts\python3.exe
```

`config.py` cũng copy tự động khi chạy script, nhưng làm tay một lần tránh lỗi
lần chạy đầu.

> **Luôn gọi** `.\.venv311\Scripts\python.exe` **khi chạy pipeline**, kể cả khi
> đã `activate`. Tránh nhầm sang Python 3.12 của hệ thống.

---

### 1.4 Tải dữ liệu CSV

Từ <https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>
tải hai file, đặt **đúng tên**:

```
E-commerce\Data\raw\2019-Oct.csv
E-commerce\Data\raw\2019-Nov.csv
```

Kiểm tra:

```powershell
Get-ChildItem Data\raw\*.csv | Select-Object Name, @{N='GB';E={[math]::Round($_.Length/1GB,2)}}
```

Kỳ vọng khoảng **5,3 GB** (Oct) và **8,4 GB** (Nov). Thiếu file → `Data_collect.py` dừng.

---

### 1.5 Checklist trước khi chạy pipeline

Mở **PowerShell mới**, `cd` vào `E-commerce`, chạy hết khối này. Mọi dòng phải
`True` / in đúng phiên bản:

```powershell
cd C:\Users\Admin\Documents\CaoHoc\BigData\E-commerce

java -version
echo "JAVA_HOME=$env:JAVA_HOME"
echo "HADOOP_HOME=$env:HADOOP_HOME"

Test-Path D:\Metadata\hadoop\bin\winutils.exe
Test-Path D:\Metadata\hadoop\bin\hadoop.dll
Test-Path "$env:JAVA_HOME\bin\hadoop.dll"

.\.venv311\Scripts\python.exe --version
Test-Path .\.venv311\Scripts\python3.exe
Test-Path Data\raw\2019-Oct.csv
Test-Path Data\raw\2019-Nov.csv

New-Item -ItemType Directory -Force -Path Data\logs | Out-Null
```

### 1.6 Nếu thiếu `winutils`

`setup_spark()` in `[WARN] Không tìm thấy winutils.exe`. Job vẫn chạy phần đọc CSV
nhưng **ghi Parquet thất bại** — `Data/bronze/` rỗng. Quay lại mục 1.2, không bỏ qua.

---

## 2. Dữ liệu

| File | Nguồn | Kích thước | Số bản ghi |
|---|---|---|---|
| `Data/raw/2019-Nov.csv` | Kaggle — E-commerce Behavior Data from Multi-category Store | 9,0 GB | 67,501,979 |
| `Data/raw/2019-Oct.csv` | cùng nguồn | 5,7 GB | 42,448,764 |

Tải tại: <https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>

Schema gốc (9 cột): `event_time`, `event_type`, `product_id`, `category_id`,
`category_code`, `brand`, `price`, `user_id`, `user_session`.

## 3. Chạy pipeline tuần tự

**Quy tắc:** chạy **từng lệnh, đợi xong, xem log**, rồi mới lệnh sau.
Không chạy song song. Script sau đọc **file trên đĩa** do script trước ghi ra.

Mở PowerShell:

```powershell
cd C:\Users\Admin\Documents\CaoHoc\BigData\E-commerce
New-Item -ItemType Directory -Force -Path Data\logs | Out-Null
```

Dùng **đúng** interpreter venv. `2>&1 | Tee-Object` vừa in ra màn hình vừa lưu log
(ghi đè log cũ — đúng ý, chỉ giữ 1 bản chạy thành công).

| # | Lệnh | Giai đoạn | Thời gian máy 16 GB | Đọc từ | Ghi ra | Thành công khi |
|---|---|---|---|---|---|---|
| 1 | `Data_collect.py` | 1.1–1.2 EDA | ~2–6 phút | `Data/raw/*.csv` | không (chỉ in console) | In schema 9 cột, ~110 triệu dòng, đếm NULL |
| 2 | `Data_cleaning.py` | 1.3 Bronze | ~12 phút | raw CSV | `Data/bronze/oct_2019.parquet`, `nov_2019.parquet` | Hai thư mục Parquet không rỗng; min/max time đúng Oct/Nov UTC |
| 3 | `Data_CDC.py` | 1.4 CDC | ~10 phút | Bronze | `Data/bronze/cdc/*.parquet` | Có `product_cdc`, `category_cdc`, `user_cdc` |
| 4 | `Data_Silver.py` | 2 Star schema | ~11 phút | Bronze + CDC | `Data/silver/{fact_event,dim_*}` | 3 kiểm tra khóa ngoại = 0 orphan |
| 5 | `Data_Verify.py` | 2 kiểm chứng | ~1–2 phút | Silver (đĩa) | không | In lại 0 orphan, timezone khớp |
| 6 | `Data_Gold.py` | 3 KPI | ~1,5 phút | Silver | `Data/gold/kpi_*` | 8 thư mục KPI |
| 7 | `Data_Dashboard.py` | 4 biểu đồ | ~1 phút | Gold | `Data/dashboard/*.png` + `*.csv` | 7 PNG phân tích + CSV |
| 8 | `Data_Architecture.py` | 4 sơ đồ | ~2 giây | (số liệu cứng trong code, khớp pipeline) | `00_*.png`, `01_mo_hinh_sao_silver.png` | 2 PNG kiến trúc |

Copy từng khối (không dán cả 8 lệnh một lúc lần đầu, dễ khó debug):

```powershell
.\.venv311\Scripts\python.exe Data_collect.py      2>&1 | Tee-Object Data\logs\collect_run.log
```

```powershell
.\.venv311\Scripts\python.exe Data_cleaning.py     2>&1 | Tee-Object Data\logs\cleaning_run.log
```

```powershell
.\.venv311\Scripts\python.exe Data_CDC.py          2>&1 | Tee-Object Data\logs\cdc_run.log
```

```powershell
.\.venv311\Scripts\python.exe Data_Silver.py       2>&1 | Tee-Object Data\logs\silver_run.log
```

```powershell
.\.venv311\Scripts\python.exe Data_Verify.py       2>&1 | Tee-Object Data\logs\verify_run.log
```

```powershell
.\.venv311\Scripts\python.exe Data_Gold.py         2>&1 | Tee-Object Data\logs\gold_run.log
```

```powershell
.\.venv311\Scripts\python.exe Data_Dashboard.py    2>&1 | Tee-Object Data\logs\dashboard_run.log
```

```powershell
.\.venv311\Scripts\python.exe Data_Architecture.py 2>&1 | Tee-Object Data\logs\architecture_run.log
```

Khi đã chắc môi trường, có thể chạy cả chuỗi (vẫn tuần tự, dừng nếu lệnh trước lỗi):

```powershell
$py = ".\.venv311\Scripts\python.exe"
& $py Data_collect.py      2>&1 | Tee-Object Data\logs\collect_run.log
& $py Data_cleaning.py     2>&1 | Tee-Object Data\logs\cleaning_run.log
& $py Data_CDC.py          2>&1 | Tee-Object Data\logs\cdc_run.log
& $py Data_Silver.py       2>&1 | Tee-Object Data\logs\silver_run.log
& $py Data_Verify.py       2>&1 | Tee-Object Data\logs\verify_run.log
& $py Data_Gold.py         2>&1 | Tee-Object Data\logs\gold_run.log
& $py Data_Dashboard.py    2>&1 | Tee-Object Data\logs\dashboard_run.log
& $py Data_Architecture.py 2>&1 | Tee-Object Data\logs\architecture_run.log
```

**Không đảo thứ tự.** `Data_Silver.py` cần `Data/bronze/cdc/`. `Data_Gold.py` cần
`Data/silver/`. `Data_Dashboard.py` cần `Data/gold/`.

Chỉ chạy lại **từ bước bị lỗi trở đi** (ví dụ sửa Gold thì không cần cleaning lại).
Muốn làm sạch tầng rồi tính lại: xóa thư mục tầng đó rồi chạy đúng script ghi tầng đó.

### 3.1 Luồng dữ liệu — mỗi script là input của script sau

```
Data/raw/*.csv
     │
     ▼  Data_collect.py        (EDA, đếm NULL)
     │
     ▼  Data_cleaning.py       → Data/bronze/*.parquet
     │
     ▼  Data_CDC.py            → Data/bronze/cdc/*.parquet
     │
     ▼  Data_Silver.py         → Data/silver/{fact_event,dim_product,dim_user,dim_category}
     │
     ▼  Data_Verify.py         (kiểm chứng, không ghi thêm)
     │
     ▼  Data_Gold.py           → Data/gold/kpi_*.parquet
     │
     ▼  Data_Dashboard.py      → Data/dashboard/*.png + *.csv
```

## 4. Cấu trúc đầu ra

```
Data/
├── raw/                              # CSV gốc, giữ nguyên định dạng nguồn
├── bronze/
│   ├── oct_2019.parquet/             # partition year=2019/month=10
│   ├── nov_2019.parquet/             # partition year=2019/month=11
│   └── cdc/
│       ├── product_cdc.parquet       # 206,876 sản phẩm, kèm change_type
│       ├── category_cdc.parquet      # tăng trưởng theo category
│       ├── user_cdc.parquet          # NEW_USER / CHURNED_USER / RETAINED
│       └── *_csv/                    # bản CSV để mở bằng Excel
├── silver/                           # mô hình sao, 4 bảng
│   ├── dim_product/                  # 1 dòng = 1 sản phẩm
│   ├── dim_user/                     # 1 dòng = 1 người dùng
│   ├── dim_category/                 # 1 dòng = 1 danh mục
│   └── fact_event/                   # 1 dòng = 1 sự kiện, partition year/month
├── gold/                             # 8 bảng KPI tổng hợp
│   ├── kpi_daily_revenue/
│   ├── kpi_category_performance/
│   ├── kpi_conversion_funnel/
│   ├── kpi_funnel_by_category/
│   ├── kpi_hourly_traffic/
│   ├── kpi_product_ranking/
│   ├── kpi_user_segment/
│   └── kpi_revenue_reconciliation/
├── dashboard/                        # 9 ảnh PNG + 8 bảng KPI CSV
└── logs/                             # 7 file log thực thi (mỗi script 1 bản thành công)
```

## 5. Các bước xử lý

### 1.1 — 1.2 · `Data_collect.py`
Đọc CSV bằng Spark, in schema, đếm số bản ghi, đếm NULL theo từng cột.

### 1.3 · `Data_cleaning.py`
- Bỏ hậu tố `" UTC"` khỏi `event_time` rồi parse theo định dạng `yyyy-MM-dd HH:mm:ss`
- Ép kiểu: `product_id`/`category_id`/`user_id` → `long`, `price` → `double`
- Fill NULL: `category_code` → `unknown.unknown`, `brand`/`user_session` → `unknown`
- Tạo cột phái sinh: `year`, `month`, `day`, `hour`, `date`, `event_date`,
  `category_level1/2/3`
- Ghi Parquet, partition theo `year`, `month`

### 1.4 · `Data_CDC.py`
So sánh hai kỳ Oct và Nov bằng `full_outer join` trên `product_id`:

| `change_type` | Ý nghĩa | Số lượng |
|---|---|---|
| `NEW_PRODUCT` | Chỉ có trong Nov | 40,082 |
| `DISCONTINUED` | Chỉ có trong Oct | 16,214 |
| `PRICE_CHANGED` | Có ở cả 2 kỳ nhưng giá khác | 46,450 |
| `UNCHANGED` | Không đổi | 104,130 |

### 2 · `Data_Silver.py`
Tách tầng Bronze thành mô hình sao 4 bảng:

| Bảng | Grain | Nội dung |
|---|---|---|
| `dim_product` | 1 sản phẩm | danh mục, thương hiệu, giá niêm yết, cờ phân loại, `change_type` từ CDC |
| `dim_user` | 1 người dùng | số phiên, số lượt mua, tổng chi, `customer_type`, `user_status` từ CDC |
| `dim_category` | 1 danh mục | 3 cấp danh mục, số sản phẩm, số sự kiện, giá trung bình |
| `fact_event` | 1 sự kiện | khóa ngoại tới 3 chiều + `event_time`, `event_type`, `price` |

Quy trình: khảo sát → phân loại danh mục → khử trùng lặp → tách chiều →
tích hợp CDC → ghi Parquet + kiểm tra khóa ngoại.

> **Về việc khôi phục `category_code`:** thiết kế ban đầu dự kiến suy ra danh mục thiếu từ
> cùng `product_id`. Đo đạc cho thấy tỉ lệ khôi phục được bằng **0** — 107.933 sản phẩm
> (56,61%) không có danh mục ở *bất kỳ* dòng nào, tức là chúng thực sự không thuộc danh mục nào.
> Đây là Missing Not At Random, không phải dữ liệu hỏng. Vì vậy thay vì điền giá trị bịa đặt,
> tầng Silver gắn cờ minh bạch `is_classified` / `is_branded` để phân tích biết chính xác
> phần nào không phân loại.

### 3 · `Data_Gold.py`
Tạo 8 bảng KPI tổng hợp, mỗi bảng trả lời một câu hỏi nghiệp vụ: doanh thu theo ngày,
hiệu quả theo danh mục, phễu chuyển đổi, lưu lượng theo giờ, xếp hạng sản phẩm,
phân khúc RFM và bảng đối chiếu doanh thu minh bạch.

> **Quy ước doanh thu:** một `purchase` = một đơn hàng. `price` là giá niêm yết của sản phẩm.
> Doanh thu = số lượt purchase × price, bỏ qua sản phẩm có `price = 0` (chưa được gán giá),
> nhưng số lượt mua vẫn được đếm đầy đủ.

### 4 · `Data_Dashboard.py`
Đọc 8 bảng Gold, xuất 7 biểu đồ PNG (doanh thu theo ngày, phễu chuyển đổi, danh mục,
lưu lượng theo giờ, phân khúc RFM, top sản phẩm, đối chiếu doanh thu) kèm bảng KPI CSV.

### 4b · `Data_Architecture.py`
Vẽ 2 sơ đồ kiến trúc bằng matplotlib (DPI 200):
- `00_kien_truc_tong_the.png` — kiến trúc Data Lake 3 tầng + luồng dữ liệu end-to-end
  (Sources → Bronze → Silver → Gold → Presentation), mũi tên ghi rõ script và thời gian chạy.
- `01_mo_hinh_sao_silver.png` — mô hình sao tầng Silver: `fact_event` ở giữa, 3 bảng chiều
  xung quanh, nhãn quan hệ khóa ngoại, khối kiểm soát chất lượng.

Sơ đồ vẽ bằng code nên số liệu ghi trên sơ đồ luôn khớp với pipeline sau mỗi lần chạy lại.

```bash
.\.venv311\Scripts\python.exe Data_Architecture.py   # sinh 2 ảnh sơ đồ
```

## 6. Cấu hình Spark

`config.py` cung cấp `setup_spark()` xử lý 4 vấn đề Windows:

| Vấn đề | Cách xử lý |
|---|---|
| Thiếu `winutils.exe` | Trỏ `HADOOP_HOME`, thêm `bin` vào `PATH` |
| Thiếu lệnh `python3` | Copy `python.exe` → `python3.exe` trong venv |
| Sai phiên bản Python | Đặt `PYSPARK_PYTHON` và `PYSPARK_DRIVER_PYTHON` |
| **Múi giờ lệch 7 tiếng** | `spark.sql.session.timeZone = UTC` |

> **Về múi giờ:** nguồn CSV ghi timestamp theo chuẩn UTC (`2019-11-01 00:00:00 UTC`).
> Nếu để Spark theo múi giờ máy (`GMT+07:00`), mọi `event_time` lệch 7 tiếng và các cột
> `day`/`hour` tính theo giờ máy sẽ mâu thuẫn nội tại với `event_time`.
> Sau khi sửa, dải thời gian trong Bronze khớp chính xác kỳ dữ liệu:
> `2019-10-01 00:00:00` → `2019-10-31 23:59:59` và `2019-11-01 00:00:00` → `2019-11-30 23:59:59`.

## 7. Ghi chú hiệu năng

Máy 16 GB RAM, 16 luồng CPU, driver heap 4 GB. Các phép đếm số user unique dùng
`approx_count_distinct` (sai số ~2%) để tránh shuffle nặng.

| Job | Thời gian |
|---|---|
| `Data_cleaning.py` | 707 giây |
| `Data_CDC.py` | 618 giây |
| `Data_Silver.py` | 668 giây |
| `Data_Verify.py` | 57 giây |
| `Data_Gold.py` | 79 giây |
| `Data_Dashboard.py` | 63 giây |

> **Không `persist()` tập dữ liệu lớn hơn RAM.** Bản đầu dùng
> `events.persist(StorageLevel.MEMORY_AND_DISK)` để tránh Spark tính lại 110 triệu dòng
> nhiều lần — nhưng Spark cache kiểu **columnar** cần > 4 GB heap và job chết với
> `OutOfMemoryError: Java heap space`. Cách đúng: **ghi `fact_event` ra đĩa một lần rồi
> đọc lại** để dựng các bảng chiều. Ngoài ra, kiểm tra chất lượng phải đọc từ đĩa
> (đúng thứ đã lưu) thay vì dùng biến trong RAM (tốn thêm hàng chục phút tính lại).

## 8. Kết quả đã kiểm chứng (24/10/2026)

| Chỉ số | Giá trị |
|---|---|
| Sự kiện Bronze → Silver | 109.950.743 → 109.819.993 (bỏ 130.750 dòng trùng) |
| Khóa mồ côi (3 quan hệ) | 0 |
| Tổng doanh thu 2 kỳ | 505.126.293 USD |
| Lượt xem / mua hàng | 104.331.840 / 1.659.703 (tỉ lệ chuyển đổi 1,591%) |
| Giá trị đơn trung bình | 304,35 USD |
| Doanh thu từ sản phẩm không phân loại | 52.801.355 USD (10,4%) |
| Sản phẩm / người dùng có mua | 226.974 / 697.470 |

**Phễu chuyển đổi:**

| Kỳ | views | carts | purchases | view→cart | cart→purchase |
|---|---|---|---|---|---|
| 10/2019 | 40.777.328 | 898.443 | 742.773 | 2,20% | 82,67% |
| 11/2019 | 63.554.512 | 2.930.007 | 916.930 | 4,61% | 31,29% |

Tháng 11 có lượt xem tăng 55,9% và lượt thêm giỏ tăng 226%, nhưng tỉ lệ
cart → purchase lại giảm mạnh — phễu rò ở bước cuối dù tổng đơn vẫn tăng.

**Phân khúc RFM:** VIP 90.918 (chi 2.214 USD TB) · Big Spender 64.531 (1.399 USD) ·
Loyal 194.732 · At Risk 254.093 · Regular 93.196.

---

## 9. Cấu trúc mã nguồn

| File | Vai trò |
|---|---|
| `config.py` | Đường dẫn, `setup_spark()`, tiện ích đo dung lượng và thời gian |
| `Data_collect.py` | Giai đoạn 1.1 và 1.2 — thu thập và EDA |
| `Data_cleaning.py` | Giai đoạn 1.3 — làm sạch, ghi tầng Bronze |
| `Data_CDC.py` | Giai đoạn 1.4 — so sánh hai kỳ |
| `Data_Silver.py` | Giai đoạn 2 — tách chiều, kiểm soát chất lượng, ghi tầng Silver |
| `Data_Verify.py` | Kiểm chứng độc lập tầng Silver từ đĩa (dòng, khóa ngoại, múi giờ) |
| `Data_Gold.py` | Giai đoạn 3 — tổng hợp KPI, ghi tầng Gold |
| `Data_Dashboard.py` | Giai đoạn 4 — xuất biểu đồ và bảng CSV |
| `Data_Architecture.py` | Vẽ sơ đồ kiến trúc tổng thể và mô hình sao Silver (PNG) |
| `NHAT_KY.md` | Nhật ký quy trình, lý do thiết kế, bài học từ lỗi |
| `PHAN_1_QUY_TRINH_HUONG_DAN.md` | Hướng dẫn triển khai chi tiết Phần 1 (Storage → Processing) |

---

## 10. Thứ tự ĐỌC mã nguồn

Không nên mở file ngẫu nhiên. Bảng dưới đây là thứ tự đọc được chọn, mỗi file chỉ dùng
được khi đã nắm file trước nó.

| # | File | Dòng | Đọc để hiểu cái gì | Mức độ cần đọc kỹ |
|---|---|---|---|---|
| 1 | `config.py` | ~170 | Nền tảng: đường dẫn, `setup_spark()`, 4 lỗi Windows, tiện ích đo | ⭐⭐⭐ Bắt buộc đọc đầu tiên |
| 2 | `Data_collect.py` | ~75 | Ý tưởng EDA, đọc CSV, đếm NULL | ⭐⭐ |
| 3 | `Data_cleaning.py` | ~213 | 5 quy tắc làm sạch, ghi tầng Bronze | ⭐⭐⭐ |
| 4 | `Data_CDC.py` | ~285 | `full_outer join`, phân loại `change_type` | ⭐⭐⭐ |
| 5 | `Data_Silver.py` | ~415 | Mô hình sao, khử trùng lặp, ghi đĩa thay vì `persist()` | ⭐⭐⭐ Khó nhất |
| 6 | `Data_Verify.py` | ~210 | Kiểm chứng độc lập, `left_anti join` | ⭐⭐ |
| 7 | `Data_Gold.py` | ~447 | 8 bảng KPI | ⭐⭐ |
| 8 | `Data_Dashboard.py` | ~340 | Vẽ 7 biểu đồ từ tầng Gold | ⭐⭐ |
| 9 | `Data_Architecture.py` | ~330 | Vẽ 2 sơ đồ kiến trúc | ⭐ |

Sau khi đọc xong mã nguồn, đọc tiếp theo thứ tự:

1. `NHAT_KY.md` — 7 lỗi đã gặp và cách xử lý (đọc trước khi sửa code)
2. `PHAN_1_QUY_TRINH_HUONG_DAN.md` — hướng dẫn triển khai chi tiết từng bước

---

## 11. Output: bảng và biểu đồ đã tạo

### 11.1 Bảng hiển thị trên CONSOLE (`.show()`) — minh chứng trực tiếp

| Script | Bảng in ra ra console |
|---|---|
| `Data_collect.py` | Schema 9 cột · 5 dòng mẫu · `describe()` · đếm NULL từng cột |
| `Data_cleaning.py` | Schema 18 cột Bronze · 5 dòng mẫu · thống kê `price` · min/max thời gian 2 kỳ |
| `Data_CDC.py` | Tổng quan 2 kỳ · phân bố `change_type` · Top 10 sản phẩm mới / bị gỡ / tăng giá · Top 10 category tăng trưởng · phân bố `user_status` |
| `Data_Silver.py` | Số dòng 4 bảng · **3 phép kiểm tra khóa ngoại** · độ bao phủ · phân bố `customer_type` / `change_type` / `user_status` · Top 10 danh mục |
| `Data_Verify.py` | Số dòng + dung lượng · đối chiếu Bronze → Silver · **3 quan hệ khóa ngoại** · độ bao phủ · kiểm tra múi giờ |
| `Data_Gold.py` | Top 10 doanh thu ngày · Top 10 danh mục · phễu chuyển đổi · lưu lượng theo giờ · Top sản phẩm · RFM |
| `Data_Dashboard.py` | Bảng tổng hợp `tong_hop_kpi.csv` (9 chỉ số) |

### 11.2 Bảng LƯU RA ĐĨA

| Tầng | Định dạng | Nội dung |
|---|---|---|
| `Data/bronze/` | Parquet | `oct_2019.parquet` (86 file, 1,62 GB) · `nov_2019.parquet` (136 file, 2,72 GB) · `cdc/` 3 bảng Parquet + 3 bảng CSV |
| `Data/silver/` | Parquet | `fact_event` (109.819.993 dòng) · `dim_product` (206.876) · `dim_user` (5.316.649) · `dim_category` (130) |
| `Data/gold/` | Parquet | **8 bảng KPI** (xem bảng bên dưới) |
| `Data/dashboard/` | **CSV** | 8 bảng KPI (7 bảng + `tong_hop_kpi.csv` tổng hợp) — mở trực tiếp bằng Excel |
| `Data/logs/` | Text | 8 file log, mỗi script đúng 1 bản chạy thành công |

8 bảng KPI ở tầng Gold:

| Bảng | Nội dung |
|---|---|
| `kpi_daily_revenue` | Doanh thu và sự kiện theo ngày |
| `kpi_category_performance` | Hiệu quả theo danh mục |
| `kpi_conversion_funnel` | Phễu view → cart → purchase |
| `kpi_funnel_by_category` | Phễu phân rã theo danh mục cấp 1 |
| `kpi_hourly_traffic` | Lưu lượng theo giờ trong ngày |
| `kpi_product_ranking` | Xếp hạng sản phẩm theo doanh thu |
| `kpi_revenue_reconciliation` | Đối chiếu doanh thu có/không phân loại |
| `kpi_user_segment` | Phân khúc RFM người dùng |

### 11.3 Biểu đồ PNG — 9 ảnh, DPI 200

| Ảnh | Nội dung |
|---|---|
| `00_kien_truc_tong_the.png` | Kiến trúc Data Lake 3 tầng, luồng dữ liệu end-to-end |
| `01_mo_hinh_sao_silver.png` | Mô hình sao tầng Silver: grain, khóa chính, khóa ngoại, số dòng |
| `01_doanh_thu_theo_ngay.png` | Doanh thu + tổng số sự kiện theo ngày, tháng 10 và 11/2019 |
| `02_phieu_chuyen_doi.png` | Phễu chuyển đổi view → cart → purchase từng tháng |
| `03_danh_muc.png` | Top 12 danh mục theo doanh thu + danh mục tăng trưởng mạnh nhất |
| `04_luu_luong_theo_gio.png` | Lưu lượng sự kiện và tỉ lệ chuyển đổi theo giờ |
| `05_phan_khung_nguoi_dung.png` | Phân khúc RFM + quan hệ thời gian không hoạt động ↔ chi tiêu |
| `06_top_san_pham.png` | Top 12 sản phẩm theo doanh thu |
| `07_doi_chieu_doanh_thu.png` | Đối chiếu doanh thu sản phẩm có/không phân loại |

### 11.4 Tầng trình bày (Presentation)

Kết quả trực quan hóa nằm ở `Data/dashboard/`:

- **9 ảnh PNG** — 7 biểu đồ nghiệp vụ (từ `Data_Dashboard.py`) + 2 sơ đồ kiến trúc
  (từ `Data_Architecture.py`)
- **8 bảng CSV** — 7 bảng KPI từ tầng Gold + `tong_hop_kpi.csv` (9 chỉ số tổng quan)

CSV mở trực tiếp bằng Excel hoặc import vào Power BI.

---

## 12. Log thực thi

Mỗi script được chạy đúng 1 lần và lưu 1 log tại `Data/logs/`:

| File log | Script | Thời gian |
|---|---|---|
| `collect_run.log` | `Data_collect.py` | 345 giây |
| `cleaning_run.log` | `Data_cleaning.py` | 707 giây |
| `cdc_run.log` | `Data_CDC.py` | 618 giây |
| `silver_run.log` | `Data_Silver.py` | 668 giây |
| `verify_run.log` | `Data_Verify.py` | 95 giây |
| `gold_run.log` | `Data_Gold.py` | 79 giây |
| `dashboard_run.log` | `Data_Dashboard.py` | 63 giây |

> `architecture_run.log` rất ngắn (~2 giây) — vẫn giữ 1 file cho đủ bộ 8 script.

> **Quy ước đặt tên:** `<script>_run.log`. Khi chạy lại pipeline, nhớ **ghi đè** log cũ
> (`Tee-Object` mặc định ghi đè) để không sinh ra nhiều bản gây rối.

---

## 13. Đưa lên Git (không đẩy data nặng)

Remote đã gắn: `https://github.com/nghiatnsimed-AI/BigData.git`.

**Vì sao không commit hết một lúc?** GitHub chặn file > 100 MB; raw CSV 5–9 GB,
Bronze/Silver Parquet ~8 GB, `.venv311` ~650 MB. Nếu GitHub Desktop stage 11.000+
file (venv + `.pyc` + `.idea`) thì push sẽ fail hoặc repo phình không dùng được.

`.gitignore` ở gốc repo **cố ý loại**:

- `.venv311/`, `__pycache__/`, `.idea/`
- `Data/raw/`, `Data/bronze/`, `Data/silver/`, `Data/gold/` (giữ `.gitkeep`)
- `kpi_user_segment.csv` (~42 MB) và `kpi_product_ranking.csv` (~19 MB)

**Thứ tự commit / push** (đã làm dần, mỗi lần một nhóm nhỏ):

| Đợt | Nội dung | Mục đích |
|---|---|---|
| 1 | `.gitignore`, README, `.gitkeep` | Chặn file nặng **trước** khi `git add` |
| 2 | 9 script `.py` + `requirements.txt` | Mã nguồn pipeline |
| 3 | `README.md`, `NHAT_KY.md`, `PHAN_1_...md` | Tài liệu |
| 4 | PNG dashboard, CSV KPI nhỏ, log, `.docx` | Minh chứng kết quả, không gồm Parquet |
| 5 | Tài liệu học phần ở thư mục gốc (nếu cần) | Đề bài / chương — file nhỏ |

Clone máy khác: tải CSV Kaggle vào `Data/raw/`, tạo venv, chạy pipeline theo mục 3.

### 13.1 Gửi cho người khác chạy code

Repo gốc: `https://github.com/nghiatnsimed-AI/BigData` — mã, tài liệu, PNG, CSV
tổng hợp, báo cáo Word đều có trên đó. Riêng **dữ liệu gốc (~14 GB)** không thể
đẩy lên Git (giới hạn 100 MB/file) nên cần gửi riêng:

| Cần gửi | Cách | Dung lượng |
|---|---|---|
| Dữ liệu gốc | Link Kaggle (tốt nhất, ai cũng tải được) | 14 GB |
| Kết quả KPI (Parquet) | Đính kèm file zip `Data/gold/` | ~20 MB |
| Biểu đồ + bảng KPI | Đã có trên Git | ~62 MB |
| Báo cáo Word | Đã có trên Git | 1,4 MB |

Người nhận làm mục 1 (Java, winutils, venv) → mục 4 (tải CSV) → mục 5 (chạy
tuần tự) là chạy được toàn bộ pipeline.


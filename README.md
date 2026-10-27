# BigData — đồ án xử lý dữ liệu lớn

Repo học phần **MTDL522**. Phần triển khai: Data Lake **Raw → Bronze → Silver → Gold**
bằng PySpark, mã nằm trong [`E-commerce/`](E-commerce/).

Tài liệu chi tiết pipeline, kiến trúc, KPI: [`E-commerce/README.md`](E-commerce/README.md).
Nhật ký lỗi / lý do thiết kế: [`E-commerce/NHAT_KY.md`](E-commerce/NHAT_KY.md).

Làm **đúng thứ tự** mục 1 → 5 trên **Windows**. Spark 3.5.3 chạy trên JVM: thiếu Java
hoặc `winutils` thì script Python vẫn mở được nhưng **không ghi được Parquet**.

---

## 1. Cần gì trên máy

| Thành phần | Phiên bản | Vì sao bắt buộc |
|---|---|---|
| Java (JDK) | **17** | Spark 3.5.3 chạy trên JVM. JDK 8 quá cũ; JDK 21+ dễ lệch API. |
| Python | **3.11** (không dùng 3.12+) | PySpark 3.5.3 cần `distutils`. Python 3.12 đã xóa → worker crash. |
| Thư viện Python | `E-commerce/requirements.txt` | `pyspark==3.5.3`, pandas, pyarrow, matplotlib |
| `winutils.exe` + `hadoop.dll` | Hadoop 3.3.x, Windows 64-bit | Windows không có `chmod`. Thiếu → Bronze rỗng. |
| RAM / đĩa | ≥ 16 GB RAM, **~25 GB trống** | Raw ~14 GB + Bronze/Silver ~9 GB + venv ~0,6 GB |

Đường dẫn máy làm đồ án (khớp `E-commerce/config.py`):

| Biến / file | Đường dẫn |
|---|---|
| `JAVA_HOME` | `D:\Metadata\jdk-17.0.12` |
| `HADOOP_HOME` | `D:\Metadata\hadoop` |
| `winutils.exe` | `D:\Metadata\hadoop\bin\winutils.exe` |
| Venv | `E-commerce\.venv311\` |

Nếu đặt JDK/Hadoop chỗ khác: sửa biến Windows **và** dòng `hadoop_home` trong
`E-commerce/config.py`.

---

## 2. Cài Java 17 (JDK)

**Bước 1 — Tải.** <https://adoptium.net/temurin/releases/?version=17>
(Eclipse Temurin / OpenJDK). Chọn Windows, x64, package **JDK**.
Dùng **`.zip`** (giải nén tay) hoặc **`.msi`**.

**Bước 2 — Giải nén (ZIP).** Tạo `D:\Metadata\` rồi giải nén sao cho có:

```
D:\Metadata\jdk-17.0.12\bin\java.exe
```

Tên thư mục có thể là `jdk-17.0.12+7` — đổi tên cho ngắn, hoặc dùng đúng tên thật
khi gán `JAVA_HOME`. **Không** trỏ `JAVA_HOME` vào `bin`.

**Bước 3 — Biến môi trường.**

1. `Win` → gõ **Environment Variables** → *Edit the system environment variables*.
2. *Environment Variables…*
3. User hoặc System:
   - **New…** → Name = `JAVA_HOME` · Value = `D:\Metadata\jdk-17.0.12`
     (thư mục có `bin\java.exe`, không thêm `\bin`).
   - Sửa `Path` → **New** → `%JAVA_HOME%\bin`
4. OK hết. **Đóng mọi PowerShell / Cursor / VS Code rồi mở lại.**

**Bước 4 — Kiểm tra** (PowerShell mới):

```powershell
java -version
echo $env:JAVA_HOME
```

Phải ra dạng `openjdk version "17.0.x"`. `JAVA_HOME` = `D:\Metadata\jdk-17.0.12`.

| Hiện tượng | Cách xử lý |
|---|---|
| `java` không phải lệnh | Chưa thêm `%JAVA_HOME%\bin` vào `Path`, hoặc chưa mở lại terminal |
| In ra Java 8 / 11 / 21 | Đưa `%JAVA_HOME%\bin` **lên đầu** `Path` |
| `JAVA_HOME` trống | Tạo đúng tên biến `JAVA_HOME` |

---

## 3. Cài `winutils.exe` + `hadoop.dll` (chỉ Windows)

Spark ghi Parquet qua Hadoop. Linux có `chmod`; Windows cần `winutils.exe`.
**Thiếu → job tính xong nhưng không ghi file.**

**Bước 1 — Tải.** Hadoop **3.3.x**, Windows 64-bit. Nguồn thường dùng:
<https://github.com/cdarlint/winutils> → `hadoop-3.3.5/bin/` (hoặc 3.3.6).
Cần `winutils.exe` và `hadoop.dll`.

**Bước 2 — Đặt đúng chỗ** (khớp `config.py`):

```
D:\Metadata\hadoop\bin\winutils.exe
D:\Metadata\hadoop\bin\hadoop.dll
```

**Bước 3 — Copy `hadoop.dll` vào JDK** (JVM load native lib từ đây):

```powershell
Copy-Item D:\Metadata\hadoop\bin\hadoop.dll $env:JAVA_HOME\bin\hadoop.dll -Force
```

**Bước 4 — Biến `HADOOP_HOME`:**

- Name = `HADOOP_HOME`
- Value = `D:\Metadata\hadoop`  ← **không** thêm `\bin`

`config.py` cũng tự gán khi thấy `winutils.exe`. Đặt biến hệ thống vẫn nên làm.

**Bước 5 — Kiểm tra:**

```powershell
Test-Path D:\Metadata\hadoop\bin\winutils.exe
Test-Path D:\Metadata\hadoop\bin\hadoop.dll
Test-Path "$env:JAVA_HOME\bin\hadoop.dll"
echo $env:HADOOP_HOME
```

Ba `Test-Path` phải `True`. `HADOOP_HOME` = `D:\Metadata\hadoop`.

---

## 4. Python 3.11, venv, dữ liệu

**Không dùng Python 3.12/3.13.**

**Bước 1 — Cài Python 3.11.** <https://www.python.org/downloads/release/python-3119/>
Tick **Add python.exe to PATH**. Kiểm tra:

```powershell
py -3.11 --version
```

**Bước 2 — Clone xong thì vào thư mục mã:**

```powershell
cd <thư-mục-clone>\BigData\E-commerce
# máy này:
cd C:\Users\Admin\Documents\CaoHoc\BigData\E-commerce

py -3.11 -m venv .venv311
.\.venv311\Scripts\Activate.ps1
python --version
```

Phải là 3.11.x. Nếu PowerShell chặn script:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
```

**Bước 3 — Thư viện:**

```powershell
python -m pip install --upgrade pip
pip install -r requirements.txt
pip show pyspark
```

`pyspark` phải là **3.5.3**.

**Bước 4 — `python3.exe`.** Spark JVM spawn worker bằng tên `python3`.
Windows không chạy `python3.bat` nếu không có shell.

```powershell
Copy-Item .\.venv311\Scripts\python.exe .\.venv311\Scripts\python3.exe
Test-Path .\.venv311\Scripts\python3.exe
```

`config.py` cũng copy khi chạy script; làm tay một lần tránh lỗi lần đầu.

> Luôn gọi `.\.venv311\Scripts\python.exe` khi chạy pipeline, kể cả đã `activate`.

**Bước 5 — CSV** (không có trên Git). Tải
<https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>
đặt đúng tên:

```
E-commerce\Data\raw\2019-Oct.csv
E-commerce\Data\raw\2019-Nov.csv
```

```powershell
Get-ChildItem Data\raw\*.csv | Select-Object Name, @{N='GB';E={[math]::Round($_.Length/1GB,2)}}
```

Kỳ vọng ~5,3 GB (Oct) và ~8,4 GB (Nov).

---

## 5. Checklist rồi chạy tuần tự

PowerShell **mới**, đang ở `E-commerce`:

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

Mọi `Test-Path` phải `True`. Thiếu `winutils` thì `setup_spark()` in `[WARN]`
và **Bronze sẽ rỗng** — quay lại mục 3, không bỏ qua.

**Quy tắc chạy:** từng lệnh, đợi xong, xem log, rồi lệnh sau. Không song song,
không đảo thứ tự. Script sau đọc file trên đĩa do script trước ghi.

| # | Script | Giai đoạn | ~thời gian (16 GB RAM) | Đọc | Ghi | Xong khi |
|---|---|---|---|---|---|---|
| 1 | `Data_collect.py` | EDA | 2–6 phút | raw CSV | không | schema 9 cột, ~110 triệu dòng |
| 2 | `Data_cleaning.py` | Bronze | ~12 phút | raw | `Data/bronze/*_2019.parquet` | Parquet không rỗng, time UTC Oct/Nov |
| 3 | `Data_CDC.py` | CDC | ~10 phút | Bronze | `Data/bronze/cdc/` | 3 bảng CDC |
| 4 | `Data_Silver.py` | Star schema | ~11 phút | Bronze + CDC | `Data/silver/` | 0 khóa mồ côi |
| 5 | `Data_Verify.py` | kiểm chứng | ~1–2 phút | Silver | không | 0 orphan, timezone khớp |
| 6 | `Data_Gold.py` | KPI | ~1,5 phút | Silver | `Data/gold/kpi_*` | 8 thư mục KPI |
| 7 | `Data_Dashboard.py` | biểu đồ | ~1 phút | Gold | `Data/dashboard/` | PNG + CSV |
| 8 | `Data_Architecture.py` | sơ đồ | ~2 giây | (số liệu trong code) | 2 PNG kiến trúc | 2 file `00_`, `01_mo_hinh_sao` |

```powershell
.\.venv311\Scripts\python.exe Data_collect.py      2>&1 | Tee-Object Data\logs\collect_run.log
.\.venv311\Scripts\python.exe Data_cleaning.py     2>&1 | Tee-Object Data\logs\cleaning_run.log
.\.venv311\Scripts\python.exe Data_CDC.py          2>&1 | Tee-Object Data\logs\cdc_run.log
.\.venv311\Scripts\python.exe Data_Silver.py       2>&1 | Tee-Object Data\logs\silver_run.log
.\.venv311\Scripts\python.exe Data_Verify.py       2>&1 | Tee-Object Data\logs\verify_run.log
.\.venv311\Scripts\python.exe Data_Gold.py         2>&1 | Tee-Object Data\logs\gold_run.log
.\.venv311\Scripts\python.exe Data_Dashboard.py    2>&1 | Tee-Object Data\logs\dashboard_run.log
.\.venv311\Scripts\python.exe Data_Architecture.py 2>&1 | Tee-Object Data\logs\architecture_run.log
```

Lần đầu: chạy **từng dòng**, không dán cả 8. Lỗi thì sửa rồi chạy lại **từ bước đó trở đi**.

Luồng:

```
Data/raw/*.csv
  → Data_collect.py          (EDA)
  → Data_cleaning.py         → Data/bronze/*.parquet
  → Data_CDC.py              → Data/bronze/cdc/
  → Data_Silver.py           → Data/silver/
  → Data_Verify.py
  → Data_Gold.py             → Data/gold/
  → Data_Dashboard.py        → Data/dashboard/*.png + *.csv
  → Data_Architecture.py     → 2 PNG kiến trúc
```

### 5.1 Chỉ cần 2 file CSV — mọi thứ còn lại tự sinh ra

**Đúng vậy.** Hai file `2019-Oct.csv` và `2019-Nov.csv` là **toàn bộ đầu vào**.
Tất cả tầng Bronze, Silver, Gold, biểu đồ, sơ đồ đều do code tự tạo ra:

| Script | Đọc vào | Tự sinh ra |
|---|---|---|
| `Data_collect.py` | `Data/raw/2019-Nov.csv` | — (chỉ in EDA ra console) |
| `Data_cleaning.py` | cả 2 file CSV | `Data/bronze/oct_2019.parquet`, `nov_2019.parquet` |
| `Data_CDC.py` | Bronze parquet | `Data/bronze/cdc/` — 3 Parquet + 3 CSV |
| `Data_Silver.py` | Bronze + CDC | `Data/silver/` — `fact_event` + 3 bảng chiều |
| `Data_Verify.py` | Silver parquet | — (chỉ kiểm chứng) |
| `Data_Gold.py` | Silver parquet | `Data/gold/` — 8 bảng KPI |
| `Data_Dashboard.py` | Gold parquet | `Data/dashboard/` — 9 PNG + 8 CSV |
| `Data_Architecture.py` | không đọc data | 2 PNG sơ đồ kiến trúc |

Các thư mục đích (`bronze/`, `silver/`, `gold/`, `dashboard/`) được script tự
tạo bằng `os.makedirs(..., exist_ok=True)` — không cần chuẩn bị trước.

**Lưu ý khi gửi:**
- Tên file phải đúng `2019-Oct.csv` / `2019-Nov.csv`, đặt trong `E-commerce/Data/raw/`.
- `Data_Architecture.py` vẽ sơ đồ từ số liệu ghi cứng trong code → sơ đồ vẫn
  đúng dù chạy trên máy khác, không phụ thuộc dữ liệu.

---

## Không đẩy lên Git

Dữ liệu pipeline **không** trên GitHub (file > 100 MB, tổng ~23 GB):

| Thư mục | Dung lượng local | Cách có lại |
|---|---|---|
| `E-commerce/Data/raw/` | ~14 GB CSV | Tải Kaggle (mục 4) |
| `E-commerce/Data/bronze/` | ~4,6 GB | Chạy `Data_cleaning.py` + `Data_CDC.py` |
| `E-commerce/Data/silver/` | ~4,2 GB | Chạy `Data_Silver.py` |
| `E-commerce/Data/gold/` | ~20 MB Parquet | Chạy `Data_Gold.py` |
| `E-commerce/.venv311/` | ~650 MB | Mục 4 |

**Có trên Git (repo ~38 MB, clone ~83 MB):** mã Spark, tài liệu, báo cáo Word,
log chạy, 9 PNG dashboard, và CSV KPI. Trong đó 2 CSV lớn nhất là
`kpi_user_segment.csv` (41 MB) và `kpi_product_ranking.csv` (19 MB) — vẫn dưới
giới hạn 100 MB nên được giữ lại làm minh chứng.

**Không có trên Git (vì quá nặng):** `Data/raw/` ~14 GB, `Data/bronze/` ~4,6 GB,
`Data/silver/` ~4,2 GB, `Data/gold/` ~20 MB, `.venv311/` ~650 MB.
Tầng Gold cũng cố ý không đẩy để repo gọn — người khác tự chạy `Data_Gold.py`.

---

## 6. Gửi cho giảng viên / người khác chạy code

Có 2 việc tách biệt: **mã nguồn** (đã có trên Git) và **dữ liệu** (không thể
đẩy lên Git vì > 100 MB).

### 6.1 Mã nguồn — gửi link GitHub

```
https://github.com/nghiatnsimed-AI/BigData
```

Giảng viên clone rồi làm mục 1 → 4 (Java, winutils, venv, CSV) là chạy được.

### 6.2 Dữ liệu — gửi riêng qua Drive/OneDrive (không qua Git)

**Phương án A (khuyến nghị):** gửi link Kaggle + hướng dẫn tải.
Bộ dữ liệu công khai, ai cũng tải được, không tốn dung lượng lưu trữ:

```
https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
```

Cách này giữ repo sạch và ai cũng tái lập được kết quả.

**Phương án B:** nén CSV rồi upload lên Google Drive / OneDrive / Dropbox,
gửi link kèm mật khẩu. Dung lượng nén 2 file CSV ~14 GB rất lớn — nên cân nhắc
chỉ gửi **kết quả đã tính sẵn** thay vì dữ liệu gốc:

| Gửi kèm | Dung lượng | Giá trị |
|---|---|---|
| `Data/dashboard/` (9 PNG + CSV) | ~62 MB | minh chứng trực quan — **đã có trên Git** |
| `Data/gold/` (8 bảng KPI Parquet) | ~20 MB | người nhận đọc KPI mà không cần Spark — **chưa có trên Git** |
| `Data/silver/` | ~4,2 GB | bảng sao đầy đủ — quá nặng, không khuyến nghị |
| `Data/raw/*.csv` | ~14 GB | chỉ cần khi muốn chạy lại toàn bộ pipeline |

> Nếu giảng viên cần **chạy code thật** → phương án A.
> Nếu giảng viên cần **xem kết quả** → Git đã đủ (PNG + CSV + báo cáo Word).

### 6.3 Kiểm tra trước khi gửi

```powershell
cd C:\Users\Admin\Documents\CaoHoc\BigData
git status                      # phải "working tree clean"
git log --oneline -3
```

Kiến trúc, KPI, nhật ký lỗi: [`E-commerce/README.md`](E-commerce/README.md).

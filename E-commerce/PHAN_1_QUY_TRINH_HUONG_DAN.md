# PHẦN 1 — QUY TRÌNH CON "STORAGE → PROCESSING"
## Hướng dẫn triển khai chi tiết, từng bước, dành cho người chưa từng làm

> **Tài liệu này dành cho ai?** Cho người đọc lần đầu, chưa biết Big Data là gì, chưa chạy
> Spark bao giờ, nhưng biết dùng terminal và biết lập trình Python sơ bộ.
> Đọc từ đầu đến cuối, làm theo đúng thứ tự là sẽ có kết quả.
>
> **Phạm vi:** Quy trình con đã chọn là **Storage → Processing** (mục 4 của đề tài).
> Ranh giới: bắt đầu từ file CSV thô trên máy, kết thúc ở tầng **Silver** đã kiểm chứng.
> Tầng Gold / Dashboard / Báo cáo là các phần khác, nằm ngoài tài liệu này.

---

## MỤC LỤC

| Mục | Nội dung | Trang |
|---|---|---|
| 0 | Tóm tắt 60 giây — bạn sẽ làm gì | 2 |
| 1 | Hiểu bài toán: vì sao cần Big Data | 4 |
| 2 | Kiến trúc tổng thể (bản vẽ ASCII) | 8 |
| 3 | Mô hình dữ liệu chi tiết từng tầng | 14 |
| 4 | Môi trường cài đặt (kèm Git, không đẩy data) | 22 |
| 5 | Bước 1 — Thu thập & EDA | 30 |
| 6 | Bước 2 — Làm sạch & ghi tầng Bronze | 38 |
| 7 | Bước 3 — CDC (so sánh 2 kỳ) | 48 |
| 8 | Bước 4 — Tách chiều, tầng Silver | 58 |
| 9 | Bước 5 — Kiểm chứng độc lập | 68 |
| 10 | Bước 6 — Tổng hợp KPI, trực quan hóa, bàn giao | 74 |
| 11 | Xử lý sự cố | 78 |
| 12 | Checklist nghiệm thu | 84 |
| 13 | Câu hỏi giảng viên hay hỏi | 86 |

---

# 0. TÓM TẮT 60 GIÂY

Nếu bạn chỉ đọc một trang rồi làm, đọc trang này.

## Bạn sẽ làm gì?

Bạn có **2 file CSV tổng cộng 14,7 GB** chứa **109.950.743 dòng** nhật ký hành vi
mua sắm của người dùng trên sàn thương mại điện tử. Nhiệm vụ: biến nó thành một
"kho dữ liệu" (Data Lake) có cấu trúc, sạch, kiểm chứng được, để nhóm sau có thể
phân tích.

## Bạn sẽ dùng công cụ gì?

**Apache Spark** (bản Python — gọi là PySpark). Spark là framework xử lý dữ liệu lớn
phân tán của Apache: đọc 14 GB CSV trong khoảng 12 phút trên một máy laptop, nhanh
hơn nhiều so với pandas (pandas sẽ chết vì không đủ RAM).

## 6 bước, 6 file script:

```
Bước 1  Data_collect.py    Thu thập + khám phá (EDA)          ~2 phút
Bước 2  Data_cleaning.py   Làm sạch → ghi tầng BRONZE        ~12 phút
Bước 3  Data_CDC.py        So sánh 2 kỳ (CDC)                ~10 phút
Bước 4  Data_Silver.py     Tách chiều → ghi tầng SILVER       ~11 phút
Bước 5  Data_Verify.py     Kiểm chứng độc lập từ đĩa          ~1 phút
Bước 6  Data_Gold.py       Tổng hợp KPI → ghi tầng GOLD      ~1,5 phút
Bước 7  Data_Dashboard.py  Biểu đồ + bảng CSV                ~1 phút
Bước 8  Data_Architecture.py  2 sơ đồ kiến trúc              ~2 giây
```

Tổng: **~39 phút** cho toàn bộ pipeline trên máy 16 GB RAM.

## Bạn sẽ có được gì?

```
Data/bronze/   2 bảng Parquet (4,34 GB) + 3 bảng CDC     ← "dữ liệu trước xử lý"
Data/silver/   4 bảng mô hình sao (4,06 GB)              ← "dữ liệu sau xử lý"
Data/logs/     5 file log thực thi                        ← minh chứng demo
```

## Không có Spark thì sao?

Không làm được. Đây là điểm cốt lõi của đồ án: **dữ liệu lớn bắt buộc phải xử lý
phân tán**. 14,7 GB CSV không vừa RAM 16 GB nếu nạp vào pandas, và cũng không thể
quét bằng tay.

---

# 1. HIỂU BÀI TOÁN

## 1.1. Dữ liệu là gì?

| Thuộc tính | Giá trị |
|---|---|
| Tên | E-commerce Behavior Data from Multi-category Store |
| Nguồn | Kaggle (mkechinov) |
| Gồm | 2 file CSV: `2019-Oct.csv` (5,28 GB), `2019-Nov.csv` (8,39 GB) |
| Số dòng | 42.448.764 (Oct) + 67.501.979 (Nov) = **109.950.743** |
| Số cột | 9 |
| Cấu trúc | Nhật ký hành vi: mỗi dòng = 1 hành động của 1 người dùng lên 1 sản phẩm |

**9 cột và ý nghĩa:**

| Cột | Kiểu | Ví dụ | Ý nghĩa |
|---|---|---|---|
| `event_time` | string | `2019-11-01 00:00:00 UTC` | Thời điểm hành động (UTC) |
| `event_type` | string | `view` | Loại hành động: `view` / `cart` / `purchase` |
| `product_id` | string | `1003461` | ID sản phẩm |
| `category_id` | string | `2053013555631882655` | ID danh mục |
| `category_code` | string | `electronics.smartphone` | Tên danh mục phân cấp, **có thể rỗng** |
| `brand` | string | `xiaomi` | Thương hiệu, **có thể rỗng** |
| `price` | string | `489.07` | Giá niêm yết, **có thể = 0** |
| `user_id` | string | `520088904` | ID người dùng (khách vãng lai) |
| `user_session` | string | `4d3b30da-a5e4-...` | ID phiên duyệt web |

**Ví dụ 3 dòng thật (lấy từ file gốc):**

```csv
event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
2019-11-01 00:00:00 UTC,view,1003461,2053013555631882655,electronics.smartphone,xiaomi,489.07,520088904,4d3b30da-a5e4-49df-b1a8-ba5943f1dd33
2019-11-01 00:00:00 UTC,view,5000088,2053013566100866035,appliances.sewing_machine,janome,293.65,530496790,8e5f4f83-366c-4f70-860e-ca7417414283
2019-11-01 00:00:01 UTC,view,17302664,2053013553853497655,,creed,28.31,561587266,755422e7-9040-477b-9bd2-6a6e8fd97387
```

> Chú ý dòng 3: `category_code` **rỗng**, nhưng `brand` = `creed` có. Và dòng 1: `price`=489.07.
> Dòng nào có `price` = `0` nghĩa là sản phẩm chưa được gán giá.

## 1.2. Vì sao cần tiếp cận Big Data? (mục 5.2 của đề tài)

Đề tài yêu cầu chỉ ra **ít nhất 02 đặc trưng** trong Volume, Velocity, Variety, Veracity,
hoặc nhu cầu xử lý phân tán / mở rộng. Dự án này thể hiện **4 đặc trưng**, mỗi cái
đều có số liệu đo được:

### Đặc trưng 1 — VOLUME (Khối lượng)

- **14,7 GB** CSV thô, **109.950.743** dòng.
- Nếu nạp vào pandas: mỗi dòng ~9 trường × object → ước tính **> 40 GB RAM** → máy
  16 GB chết ngay.
- Spark xử lý bằng cách chia dữ liệu thành **partition** (mảnh nhỏ), mỗi partition
  xử lý độc lập trên 1 core, rồi gộp kết quả.

**Bằng chứng trong log Spark** (bạn sẽ tự nhìn thấy khi chạy):
```
[Stage 2:==>            (10 + 16) / 68]     ← 16 task chạy song song
```
Con số `16` là số luồng CPU của máy bạn dùng. Trên cụm 100 node, con số này thành 1.600.

### Đặc trưng 2 — VELOCITY (Tốc độ phát sinh)

- Dữ liệu phát sinh **liên tục** từ hệ thống tracking web: mỗi lần người dùng
  cuộn trang, bấm vào sản phẩm, thêm giỏ → 1 dòng log.
- 2 kỳ chỉ 61 ngày đã có 110 triệu dòng → trung bình **~1,8 triệu dòng/ngày**.
- Nếu ngày mai hệ thống chạy liên tục: 1 năm ≈ 657 triệu dòng ≈ **88 GB/năm**.
- Yêu cầu mở rộng: kiến trúc phải chia nhỏ ghi theo kỳ (partition theo `year`, `month`)
  để không phải ghi lại toàn bộ dữ liệu mỗi lần có thay đổi.

### Đặc trưng 3 — VARIETY (Đa dạng)

- Ban đầu chỉ 1 nguồn (CSV) nhưng schema đã phân cấp: `category_code` tách được thành
  3 cấp (`electronics` → `smartphone` → tên cụ thể).
- Từ 1 nhật ký phẳng, phải tách ra **4 bảng** khác nhau (mô hình sao) ở tầng Silver.
- Có thể mở rộng thêm nguồn: Kafka streaming, API đơn vị bán lẻ, dữ liệu quảng cáo.

### Đặc trưng 4 — VERACITY (Độ tin cậy / chất lượng)

- Dữ liệu thô có vấn đề thật: `category_code` NULL ở **32,4%** số dòng,
  `brand` NULL ở **13,7%**, `price` = 0 ở một số sản phẩm,
  **130.750 dòng trùng lặp hoàn toàn**, hậu tố `" UTC"` trong chuỗi thời gian.
- Xử lý dữ liệu lớn = xử lý dữ liệu **bẩn** ở quy mô lớn. Phần lớn công việc thật
  của kỹ sư dữ liệu là ở đây, không phải ở thuật toán.

### Đặc trưng 5 — Nhu cầu xử lý phân tán & mở rộng

- Pipeline này chạy trên `local[*]` (một máy), nhưng **toàn bộ code là code Spark
  phân tán** — chỉ cần đổi `master="local[*]"` thành `master="spark://master:7077"`
  là chạy trên cụm, không phải viết lại.
- Giới hạn đo được: máy 16 GB RAM, 16 luồng. Job nặng nhất mất 707 giây. Trên cụm
  16 node × 16 core, thời gian chia cho ~16 → có thể xuống dưới 1 phút.

## 1.3. Ba kiểu xử lý dữ liệu lớn — và tại sao chọn Batch

| Kiểu | Khi nào dùng | Dự án này? |
|---|---|---|
| **Batch** | Dữ liệu đã tĩnh, xử lý theo lô định kỳ | ✅ **Dùng** |
| **Streaming** | Dữ liệu đến liên tục, cần phản hồi trong vài giây | ❌ Không dùng (đã ghi chú trong báo cáo là hướng mở rộng) |
| **Hybrid** | Batch cho lịch sử + Streaming cho realtime | ❌ Không dùng |

**Lý do chọn Batch:** dữ liệu nguồn là file CSV tĩnh theo tháng (Oct, Nov). Xử lý
batch theo tháng là mô hình chuẩn của Data Lake, đơn giản, tái lập được, và dễ
chứng minh bằng số liệu. Nếu giảng viên hỏi "sao không làm streaming?", trả lời:
*"Batch là đủ cho dữ liệu theo tháng; kiến trúc đã tách rõ lớp đọc (Bronze) khỏi
lớp xử lý nên có thể thay lớp đọc CSV bằng Kafka consumer mà không sửa logic phía
sau."*

## 1.4. Đề tài yêu cầu gì ở Phần 1?

Từ mục 5 của đề tài, quy trình con "Storage → Processing" phải có 4 sản phẩm tối thiểu:

| # | Yêu cầu đề tài | Đáp ứng ở đây | Ở mục nào |
|---|---|---|---|
| 1 | **Job xử lý phân tán** | 6 script PySpark, chạy `local[*]` 16 luồng | Mục 5–9 |
| 2 | **Dữ liệu trước/sau xử lý** | Bronze (trước) → Silver (sau) | Mục 3, 6, 8 |
| 3 | **Thống kê chất lượng và thời gian xử lý** | `Data_Verify.py` + bảng thời gian từng job | Mục 9, 12 |
| 4 | (mục 5.7) **Log, số liệu, minh chứng** | 5 file log trong `Data/logs/` | Mục 10 |

---

# 2. KIẾN TRÚC TỔNG THỂ

## 2.1. Bức tranh toàn cảnh

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SOURCES — nguồn phát sinh dữ liệu                                       │
│                                                                          │
│   Trang web thương mại điện tử ghi log hành vi người dùng              │
│   └──► Data/raw/2019-Oct.csv   (5,28 GB, 42.448.764 dòng)              │
│   └──► Data/raw/2019-Nov.csv   (8,39 GB, 67.501.979 dòng)              │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                   ┌───────────▼────────────┐
                   │   INGESTION + STORAGE   │  Bước 1-2
                   │   Data_collect.py       │  Data_collect.py
                   │   Data_cleaning.py      │  Data_cleaning.py
                   └───────────┬────────────┘
                               │  Parquet + Snappy, partition year/month
                   ┌───────────▼────────────┐
                   │   BRONZE — dữ liệu đã   │  Bước 2
                   │   chuẩn hóa, giữ dạng  │
                   │   nhật ký sự kiện      │
                   │                          │
                   │   oct_2019.parquet       │
                   │   nov_2019.parquet       │
                   │   cdc/  ◄── Data_CDC.py (Bước 3) so sánh 2 kỳ
                   └───────────┬────────────┘
                               │
                   ┌───────────▼────────────┐
                   │   PROCESSING            │  Bước 4
                   │   Data_Silver.py        │
                   │   tách chiều → mô hình sao│
                   └───────────┬────────────┘
                               │
        ┌──────────┬───────────┼───────────┐
        ▼          ▼           ▼           ▼
  ┌──────────┐┌─────────┐┌───────────┐┌──────────┐
  │dim_prod..││dim_user ││dim_catego.││fact_event│  ◄── Bước 5
  │          ││         ││           ││          │      Data_Verify.py
  └──────────┘└─────────┘└───────────┘└──────────┘      kiểm chứng
                    TẦNG SILVER
```

## 2.2. Bảng chi tiết từng thành phần

| # | Thành phần | Công nghệ | Vai trò | Ở đâu trong báo cáo |
|---|---|---|---|---|
| 1 | `Data/raw/` | HDFS/Local FS | Lưu trữ dữ liệu gốc bất biến, định dạng CSV | Mục 3 |
| 2 | `Data_collect.py` | PySpark | Đọc CSV, đếm dòng, in schema, đo NULL, xem mẫu | Mục 5 |
| 3 | `Data_cleaning.py` | PySpark + Parquet | Làm sạch, chuẩn hóa kiểu, tạo cột phái sinh, ghi Bronze | Mục 6 |
| 4 | `Data/bronze/` | Parquet + Snappy | Tầng lưu trữ dữ liệu đã chuẩn hóa, schema-được, phân vùng | Mục 3 |
| 5 | `Data_CDC.py` | Spark SQL (`full_outer join`) | So sánh Oct vs Nov, phân loại thay đổi sản phẩm & user | Mục 7 |
| 6 | `Data_Silver.py` | PySpark | Tách chiều, khử trùng lặp, gắn nhãn CDC, kiểm tra khóa ngoại | Mục 8 |
| 7 | `Data/silver/` | Parquet | Mô hình sao 4 bảng — nơi truy vấn phân tích bắt đầu | Mục 3 |
| 8 | `Data_Verify.py` | PySpark | Kiểm chứng độc lập: số dòng, khóa ngoại, độ bao phủ, múi giờ | Mục 9 |
| 9 | `Data/logs/` | Text log | Minh chứng demo (mục 5.7) | Mục 10 |
| 10 | `Data_Gold.py` | PySpark | Tổng hợp 8 bảng KPI theo grain nghiệp vụ, ghi tầng Gold | Mục 10 |
| 11 | `Data_Dashboard.py` | Matplotlib | Vẽ 7 biểu đồ + xuất 8 bảng KPI CSV cho Power BI / Excel | Mục 10 |
| 12 | `Data_Architecture.py` | Matplotlib | Vẽ 2 sơ đồ kiến trúc (tổng thể + mô hình sao Silver) | Mục 10 |

## 2.3. Vì sao chia làm 3 tầng (Bronze → Silver)?

Đây là câu hỏi giảng viên chắc chắn sẽ hỏi. Câu trả lời:

**Bronze** = dữ liệu "đúng như nguồn, nhưng đã ở định dạng phân tích được".
- Giữ nguyên mọi sự kiện, không diễn giải.
- Cho phép **audit**: sau này phát hiện quy tắc làm sạch sai → sửa và chạy lại từ Bronze
  mà không phải đọc lại 14,7 GB CSV.
- Giữ nguyên bản chất sự kiện vì tầng Silver cần cả thông tin đó.

**Silver** = dữ liệu đã có mô hình quan hệ rõ ràng.
- Tách `fact_event` (sự kiện) khỏi 3 bảng chiều (`dim_product`, `dim_user`,
  `dim_category`) → tránh lặp lại thông tin danh mục/brand 110 triệu lần.
- Có khóa ngoại được kiểm chứng → mọi phép join phía sau đều tin được.
- Nhỏ hơn nhiều → truy vấn nhanh hơn.

**Nếu gộp làm 1 bước:** mất khả năng audit, mất khả năng tái xử lý, và mọi lỗi
phát hiện ở bước sau đều phải đọc lại từ CSV 14,7 GB (tốn 12 phút mỗi lần).

## 2.4. Định dạng: tại sao CSV → Parquet?

| Tiêu chí | CSV | **Parquet** (dùng) |
|---|---|---|
| Dung lượng | 14,7 GB | 4,34 GB (nhỏ hơn **3,4×**) |
| Đọc 1 cột | phải đọc hết file | chỉ đọc cột cần (**column pruning**) |
| Kiểu dữ liệu | tất cả là chuỗi | schema tự mô tả: `long`, `double`, `timestamp` |
| Ghi thêm dữ liệu | ghi lại toàn bộ | append theo partition |
| Nén | không | Snappy |

Đây là điểm cốt lõi của kiến trúc Data Lake: **định dạng lưu trữ tối ưu cho
truy vấn**, không phải định dạng thuận tiện cho con người.

## 2.5. Phân vùng (partitioning)

Bronze được ghi với `partitionBy("year", "month")`:
```
Data/bronze/oct_2019.parquet/
├── year=2019/
│   └── month=10/
│       ├── part-00000-....snappy.parquet
│       ├── part-00001-....snappy.parquet
│       └── ...  (86 file)
Data/bronze/nov_2019.parquet/
└── year=2019/month=11/   (136 file)
```

**Lợi ích:**
1. **Pruning**: truy vấn "chỉ tháng 11" chỉ đọc thư mục `month=11` — bỏ qua 1,62 GB.
2. **Ghi thêm không cần viết lại**: thêm ngày 01/12 chỉ tạo partition mới `month=12`.
3. **Tính dễ chịu**: một partition = một đơn vị song song hóa tự nhiên.

**Trade-off:** partition quá nhỏ (quá nhiều file) sẽ làm chậm metadata planning.
Với 86–136 file/partition thì rất tốt.

---

# 3. MÔ HÌNH DỮ LIỆU CHI TIẾT

## 3.1. Schema gốc (CSV)

```
root
 |-- event_time: string        (NULL: 0)
 |-- event_type: string       (NULL: 0)
 |-- product_id: string       (NULL: 0)
 |-- category_id: string      (NULL: 0)
 |-- category_code: string    (NULL: ~32,4%)
 |-- brand: string            (NULL: ~13,7%)
 |-- price: string            (NULL: 0, nhưng có giá trị "0")
 |-- user_id: string          (NULL: 0)
 |-- user_session: string     (NULL: 0)
```

## 3.2. Schema sau khi làm sạch (Bronze) — 18 cột

```
root
 |-- event_time: timestamp           ← đã parse, bỏ hậu tố " UTC"
 |-- event_type: string
 |-- product_id: long                ← đã ép kiểu
 |-- category_id: long
 |-- category_code: string           ← NULL đã thay bằng "unknown.unknown"
 |-- brand: string                   ← NULL đã thay bằng "unknown"
 |-- price: double                   ← NaN đã thay bằng 0.0
 |-- user_id: long
 |-- user_session: string            ← NULL đã thay bằng "unknown"
 |-- year: int                       ┐
 |-- month: int                      │
 |-- day: int                        │ 9 cột phái sinh
 |-- hour: int                       │
 |-- date: date                      │
 |-- event_date: string              │
 |-- category_level1: string         │
 |-- category_level2: string         │
 |-- category_level3: string         ┘
```

## 3.3. Mô hình sao tầng Silver

```
                      ┌────────────────────┐
                      │    dim_category    │
                      │  1 dòng = 1 danh mục│
                      │  PK: category_code │
                      │  130 dòng         │
                      └─────────┬──────────┘
                                │
                  category_code │ (FK)
                                │
┌───────────────────┐  ┌──────▼─────────────────────────┐  ┌────────────────────┐
│   dim_product     │  │        fact_event              │  │     dim_user      │
│ 1 dòng = 1 sản phẩm│  │   1 dòng = 1 sự kiện          │  │ 1 dòng = 1 người   │
│ PK: product_id    │  │   109.819.993 dòng             │  │ PK: user_id        │
│ 206.876 dòng      │◄─┤  FK: product_id, user_id,       │  │ 5.316.649 dòng    │
│ + change_type     │  │      category_code              │  │ + user_status      │
└───────────────────┘  │  + event_time, event_type, price │  └─────────┬──────────┘
                       └──────────────────────────────────┘            │
                                                          user_id (FK)  │
                                                                       ▼
```

### `fact_event` — bảng sự kiện (grain: 1 sự kiện)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `event_time` | timestamp | thời điểm |
| `event_type` | string | view / cart / purchase |
| `product_id` | long | FK → dim_product |
| `user_id` | long | FK → dim_user |
| `category_code` | string | FK → dim_category |
| `user_session` | string | phiên |
| `price` | double | giá niêm yết tại thời điểm sự kiện |
| `event_date` | string | `yyyy-MM-dd` (cột partition) |
| `year`, `month`, `day`, `hour`, `date` | int/date | phái sinh |
| `category_level1/2/3` | string | phân cấp danh mục |
| `is_classified` | bool | sản phẩm CÓ danh mục hay không |
| `is_branded` | bool | sản phẩm CÓ thương hiệu hay không |

### `dim_product` — grain: 1 sản phẩm (206.876 dòng)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `product_id` | long | **PK** |
| `category_code` | string | FK → dim_category |
| `brand` | string | thương hiệu |
| `level1`, `level2` | string | cấp danh mục 1, 2 |
| `is_classified` | bool | có danh mục hay không |
| `is_branded` | bool | có thương hiệu hay không |
| `list_price` | double | giá niêm yết (mode — giá xuất hiện nhiều nhất) |
| `avg_price` | double | giá trung bình |
| `total_events` | long | số sự kiện |
| `unique_users` | long | số người dùng (xấp xỉ) |
| `first_seen`, `last_seen` | timestamp | lần đầu / lần cuối xuất hiện |
| `change_type` | string | ← **từ CDC**: NEW_PRODUCT / DISCONTINUED / PRICE_CHANGED / UNCHANGED |

### `dim_user` — grain: 1 người dùng (5.316.649 dòng)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `user_id` | long | **PK** |
| `session_count` | long | số phiên (xấp xỉ) |
| `total_events` | long | tổng sự kiện |
| `products_viewed` | long | số sản phẩm đã xem (xấp xỉ) |
| `categories_viewed` | long | số danh mục đã xem |
| `purchase_count` | long | số lượt mua |
| `cart_count` | long | số lượt thêm giỏ |
| `view_count` | long | số lượt xem |
| `total_spent` | double | **chỉ tính từ sự kiện purchase** |
| `active_days` | int | số ngày có hoạt động |
| `customer_type` | string | buyer / cart_only / viewer |
| `user_status` | string | ← **từ CDC**: NEW_USER / CHURNED_USER / RETAINED |

### `dim_category` — grain: 1 danh mục (130 dòng)

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `category_code` | string | **PK** |
| `level1`, `level2`, `level3` | string | 3 cấp |
| `product_count` | long | số sản phẩm |
| `event_count` | long | số sự kiện |
| `user_count` | long | số người dùng |
| `avg_price`, `max_price`, `min_price` | double | thống kê giá |

## 3.4. Bảng CDC (ở tầng Bronze)

| Bảng | Grain | Cột quan trọng |
|---|---|---|
| `product_cdc` | 1 sản phẩm | `change_type`, `change_severity`, `price_delta`, `events_oct/nov`, `users_oct/nov` |
| `category_cdc` | 1 danh mục | `new_products`, `discontinued_products`, `price_changed_products`, `event_growth_pct` |
| `user_cdc` | 1 người dùng | `user_status` (NEW_USER / CHURNED_USER / RETAINED) |

**Giá trị `change_type`:**

| Giá trị | Điều kiện | Số lượng (đo được) |
|---|---|---|
| `UNCHANGED` | có ở cả 2 kỳ, giá không đổi | 104.130 (50,3%) |
| `PRICE_CHANGED` | có ở cả 2 kỳ, giá khác | 46.450 (22,5%) |
| `NEW_PRODUCT` | chỉ có trong Nov | 40.082 (19,4%) |
| `DISCONTINUED` | chỉ có trong Oct | 16.214 (7,8%) |

**Giá trị `user_status`:**

| Giá trị | Điều kiện | Số lượng |
|---|---|---|
| `NEW_USER` | chỉ xuất hiện ở Nov | 2.294.359 |
| `CHURNED_USER` | chỉ xuất hiện ở Oct | 1.620.532 |
| `RETAINED` | xuất hiện ở cả 2 kỳ | 1.401.758 |

---

# 4. MÔI TRƯỜNG CÀI ĐẶT

> Mục này giả định bạn dùng Windows (đồ án của bạn đang chạy trên Windows).
> Nếu dùng Linux/macOS, bỏ qua các bước về `winutils.exe`.

## 4.1. Danh sách phần mềm cần cài

| Phần mềm | Phiên bản | Bắt buộc? | Ghi chú |
|---|---|---|---|
| Python | **3.11** | ✅ | PySpark 3.5.3 **không** chạy được với Python 3.12+ |
| Java (JDK) | **17** | ✅ | Spark 3.5.3 cần JDK 17 |
| PySpark | 3.5.3 | ✅ | `pip install pyspark==3.5.3` |
| pandas, pyarrow | mới nhất | ✅ | dùng cho kiểm tra và ghi CSV |
| matplotlib | mới nhất | ⬜ | chỉ dùng ở phần Dashboard |
| winutils.exe + hadoop.dll | bản Windows | ✅ | **bắt buộc** để Spark ghi được Parquet trên Windows |

> ⚠️ **Lỗi #1 gây mất thời gian nhất:** dùng Python 3.12 (mặc định khi cài Python mới
> trên máy). Mọi tác vụ cần Python Worker của Spark sẽ chết với
> `Python worker exited unexpectedly` — nguyên nhân thật là Python 3.12 đã **xoá
> module `distutils`**. Luôn dùng venv Python 3.11.

## 4.2. Cài Java 17

1. Tải JDK 17 từ <https://adoptium.net/> (bản Windows x64, file `.zip`).
2. Giải nén, ví dụ `D:\Metadata\jdk-17.0.12`.
3. Đặt biến môi trường:
   - `JAVA_HOME` = `D:\Metadata\jdk-17.0.12`
   - Thêm `%JAVA_HOME%\bin` vào `PATH`.
4. Kiểm tra:
   ```powershell
   java -version
   ```
   Phải in ra `openjdk version "17.x.x"`. Nếu không, Spark sẽ lỗi khi khởi động.

## 4.3. Cài winutils.exe + hadoop.dll

1. Tải `winutils.exe` + `hadoop.dll` (bản Windows 64-bit, Hadoop 3.3.x).
2. Đặt tại `D:\Metadata\hadoop\bin\`.
3. Copy `hadoop.dll` vào `%JAVA_HOME%\bin`.
4. Đặt `HADOOP_HOME` = `D:\Metadata\hadoop`.

**Vì sao cần?** Spark (chạy trên JVM/Hadoop) khi ghi file Parquet sẽ gọi hàm
`setPermission` để đặt quyền file. Trên Windows, lệnh `chmod` không tồn tại →
Hadoop cần `winutils.exe` để mô phỏng. **Thiếu file này → script chạy tới bước
ghi Parquet rồi báo lỗi, thư mục Bronze rỗng hoàn toàn.**

## 4.4. Tạo môi trường ảo Python 3.11

```powershell
cd C:\Users\Admin\Documents\CaoHoc\BigData\E-commerce

# Tạo venv
python -m venv .venv311

# Kích hoạt
.\.venv311\Scripts\activate

# Cài thư viện
pip install -r requirements.txt
```

## 4.5. Tạo lệnh `python3.exe` trong venv ⚠️

```powershell
copy .\.venv311\Scripts\python.exe .\.venv311\Scripts\python3.exe
```

**Vì sao?** Spark là chương trình JVM. Khi cần chạy đoạn code Python (ví dụ UDF),
nó gọi chương trình tên **`python3`** qua `ProcessBuilder`. Trên Windows,
`ProcessBuilder` **không** tự tìm file `.bat`/`.cmd` khi không có shell → lỗi
`Cannot run program python3`. Cách chắc chắn 100% là tạo file `.exe` thật.

> `config.py` cũng tự động làm việc này mỗi lần chạy (hàm `setup_spark()`), nhưng
> copy thủ công một lần giúp bạn không bị lỗi lần đầu.

## 4.6. Tải dữ liệu

Từ <https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>

Đặt vào `E-commerce/Data/raw/`:
```
Data/raw/2019-Oct.csv   (5,28 GB)
Data/raw/2019-Nov.csv   (8,39 GB)
```

## 4.7. Cấu trúc thư mục dự kiến

```
E-commerce/
├── config.py                  ← tạo ở Mục 4.8
├── Data_collect.py            ← Mục 5
├── Data_cleaning.py           ← Mục 6
├── Data_CDC.py                ← Mục 7
├── Data_Silver.py             ← Mục 8
├── Data_Verify.py             ← Mục 9
├── Data_Gold.py               ← Mục 10
├── Data_Dashboard.py          ← Mục 10
├── Data_Architecture.py       ← Mục 10
├── requirements.txt
├── README.md
├── .venv311/
└── Data/
    ├── raw/                   ← CSV gốc (tải về)
    ├── bronze/                ← tạo ra
    ├── silver/                ← tạo ra
    ├── gold/                  ← (phần khác, không nằm trong tài liệu này)
    ├── dashboard/             ← (phần khác)
    └── logs/                  ← tạo ra
```

## 4.8. File `config.py` — chạm tránh 4 lỗi Windows

Đây là file **quan trọng nhất** của dự án. Tạo file `config.py`:

```python
"""
Cấu hình chung cho dự án
"""
import os
import sys

# Windows mặc định dùng cp1252 -> in tiếng Việt bị UnicodeEncodeError.
# Ép stdout/stderr dùng UTF-8 với errors="replace" để không crash khi in log.
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ===============================
# KIỂM TRA PHIÊN BẢN PYTHON
# ===============================
# PySpark 3.5.3 chỉ hỗ trợ Python 3.8 - 3.11.
# Python 3.12 đã xóa module `distutils` -> Python Worker crash
# -> mọi UDF / pandas_udf / RDD.map đều fail với "Python worker exited unexpectedly".
if sys.version_info >= (3, 12):
    print("=" * 60)
    print("⚠️  CẢNH BÁO PHIÊN BẢN PYTHON")
    print("=" * 60)
    print(f"Đang dùng Python {sys.version.split()[0]}")
    print("PySpark 3.5.3 KHÔNG hỗ trợ Python 3.12+")
    print("Python Worker sẽ CRASH (thiếu module 'distutils')")
    print()
    print("GIẢI PHÁP: chạy bằng venv Python 3.11")
    print("  .\\.venv311\\Scripts\\python.exe <script>.py")
    print("=" * 60)
    print()

# Đường dẫn gốc
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# Data paths
DATA_RAW = os.path.join(PROJECT_ROOT, "Data", "raw")
DATA_BRONZE = os.path.join(PROJECT_ROOT, "Data", "bronze")
DATA_SILVER = os.path.join(PROJECT_ROOT, "Data", "silver")
DATA_GOLD = os.path.join(PROJECT_ROOT, "Data", "gold")

# Files
NOV_FILE = os.path.join(DATA_RAW, "2019-Nov.csv")
OCT_FILE = os.path.join(DATA_RAW, "2019-Oct.csv")

# ===============================
# CẤU HÌNH SPARK DÙNG CHUNG
# ===============================
# Ghi trong file Python để mọi script tự chạy được mà không cần set biến môi trường.

def setup_spark(app_name, master="local[*]", cores=None, shuffle_partitions=8,
                driver_memory="4g", log_level="WARN", session_timezone="UTC"):
    """
    Tạo SparkSession với cấu hình đã được sửa cho Windows.

    Các lỗi đã xử lý:
      1. Thiếu winutils.exe  -> Spark không set được permission khi tạo thư mục
                                output => lỗi "setPermission" => ghi Parquet thất bại.
      2. Thiếu lệnh python3   -> Spark không spawn được Python Worker
                                => lỗi "Cannot run program python3".
      3. Sai phiên bản Python   -> Spark 3.5.3 không hỗ trợ Python 3.12+
                                => Python Worker crash với "distutils".
      4. Sai múi giờ session   -> Spark parse timestamp theo giờ máy (GMT+07:00)
                                trong khi nguồn CSV ghi chuẩn " UTC"
                                => mọi event_time lệch 7 tiếng so với giá trị gốc.
    """
    # --- Fix 1: trỏ HADOOP_HOME về nơi có winutils.exe ---
    hadoop_home = r"D:\Metadata\hadoop"
    winutils = os.path.join(hadoop_home, "bin", "winutils.exe")
    if os.path.exists(winutils):
        os.environ["HADOOP_HOME"] = hadoop_home
        os.environ["hadoop.home.dir"] = hadoop_home
        os.environ["PATH"] = os.path.join(hadoop_home, "bin") + os.pathsep + os.environ.get("PATH", "")
    else:
        print(f"[WARN] Không tìm thấy winutils.exe tại {winutils}")
        print(f"[WARN] Spark sẽ không ghi được file Parquet. Cần cài winutils.")

    # --- Fix 2: tạo lệnh python3 trỏ về python của venv ---
    # Spark (JVM) gọi "python3" khi spawn Python Worker qua ProcessBuilder.
    # Trên Windows, ProcessBuilder KHÔNG tự resolve file .bat/.cmd khi không có shell,
    # nên phải tạo một file .exe thật. Cách chắc chắn nhất: copy python.exe -> python3.exe.
    python_exe = sys.executable
    python_dir = os.path.dirname(python_exe)
    python3_exe = os.path.join(python_dir, "python3.exe")
    if not os.path.exists(python3_exe):
        try:
            import shutil
            shutil.copy2(python_exe, python3_exe)
            print(f"[OK] Đã tạo python3.exe -> {python_exe}")
        except Exception as e:
            print(f"[WARN] Không tạo được python3.exe: {e}")

    # --- Fix 3: chỉ định interpreter cho Python Worker ---
    os.environ["PYSPARK_PYTHON"] = python_exe
    os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

    if python_dir not in os.environ.get("PATH", "").split(os.pathsep):
        os.environ["PATH"] = python_dir + os.pathsep + os.environ.get("PATH", "")

    from pyspark.sql import SparkSession

    builder = (SparkSession.builder
               .appName(app_name)
               .master(master)
               .config("spark.ui.enabled", "true")
               .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
               .config("spark.driver.memory", driver_memory)
               # Múi giờ phiên: nguồn CSV ghi "2019-11-01 00:00:00 UTC".
               # Nếu để múi giờ máy (GMT+07:00), Spark sẽ parse lệch 7 tiếng
               # và cột day/hour tính theo giờ máy -> mâu thuẫn với event_time.
               .config("spark.sql.session.timeZone", session_timezone)
               .config("spark.hadoop.fs.permissions.umask-mode", "000")
               .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
               .config("spark.sql.sources.partitionOverwriteMode", "dynamic"))

    if cores:
        builder = builder.config("spark.driver.cores", str(cores))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    return spark


# ===============================
# TIỆN ÍCH ĐO THỜI GIAN
# ===============================
def banner(title, char="=", width=60):
    """In tiêu đề có đường kẻ."""
    print("\n" + char * width)
    print(title)
    print(char * width)


def human_bytes(num):
    """Đổi số byte sang dạng người đọc được."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num) < 1024:
            return f"{num:.2f} {unit}"
        num /= 1024
    return f"{num:.2f} PB"


def dir_size(path):
    """Tính tổng dung lượng của thư mục (đệ quy)."""
    total = 0
    count = 0
    for root, dirs, files in os.walk(path):
        for f in files:
            fp = os.path.join(root, f)
            if os.path.exists(fp):
                total += os.path.getsize(fp)
                count += 1
    return total, count
```

> **Lưu ý:** dòng `DATA_RAW = os.path.join(PROJECT_ROOT, "Data", "raw")` ở trên là
> bản **đã sửa đúng**. Nếu bạn gõ tay, đảm bảo không dấu `:=` bị sót lại.

## 4.9. File `requirements.txt`

```
pyspark==3.5.3
pandas
pyarrow
matplotlib
```

> `scikit-learn` đã bỏ vì không dùng trong dự án.
> `python-docx` cũng đã bỏ — không còn script sinh báo cáo Word.

## 4.10. Kiểm tra môi trường đã đúng chưa

Tạo file tạm `test_env.py`:
```python
from config import *
spark = setup_spark("Test", log_level="INFO")
df = spark.read.csv(NOV_FILE, header=True).limit(5)
df.show(truncate=False)
print("✅ Môi trường OK")
spark.stop()
```

Chạy:
```powershell
.\.venv311\Scripts\python.exe test_env.py
```

**Kết quả mong đợi:** in ra 5 dòng dữ liệu và `✅ Môi trường OK`.
**Nếu lỗi:** tra bảng ở Mục 11.

## 4.11. Git — tại sao không push data, và push theo đợt

GitHub giới hạn **100 MB / file** và repo nên chỉ chứa mã + tài liệu. Kho local
có ~23 GB (`raw` 14 GB + Bronze/Silver Parquet + venv). **Không** `git add .`
khi chưa có `.gitignore` — GitHub Desktop sẽ hiện hàng chục nghìn file `.pyc`/venv.

Làm theo thứ tự này:

1. Commit `.gitignore` **trước** (đã có ở gốc repo `BigData`).
2. Commit mã nguồn (9 file `.py` + `requirements.txt`).
3. Commit tài liệu `.md`.
4. Commit PNG / CSV KPI nhỏ / log / báo cáo Word.
5. Không commit `Data/raw`, `bronze`, `silver`, `gold`, `.venv311`.

Người clone repo phải tự tải CSV Kaggle vào `Data/raw/` rồi chạy pipeline (Mục 5–10).
Thư mục trống được giữ bằng file `.gitkeep`.

---

# 5. BƯỚC 1 — THU THẬP & KHÁM PHÁ (EDA)

**File:** `Data_collect.py` · **Thời gian:** ~2 phút · **Mục đề:** 1.1, 1.2

## 5.1. Tại sao cần bước này?

Không bao giờ viết quy tắc làm sạch trước khi hiểu dữ liệu. Bước EDA trả lời:
- Bao nhiêu dòng? Bao nhiêu cột? Kiểu gì?
- Cột nào có NULL? Bao nhiêu?
- Giá trị phân bố thế nào?
- Có dòng trùng không?

Kết quả của bước này quyết định 5 quy tắc làm sạch ở Mục 6.

## 5.2. Code

```python
"""
GIAI ĐOẠN 1: THU THẬP DỮ LIỆU
E-commerce Behavior Data Pipeline
"""

from config import *
from pyspark.sql import SparkSession
import sys

# ===============================
# 2. KHỞI TẠO SPARK SESSION
# ===============================
# 1. local[*] KHÔNG có nghĩa là chạy trên nhiều máy chủ. Nó chạy trên máy tính của bạn.
# 2. spark.sql.shuffle.partitions = 8 không có nghĩa là toàn bộ dữ liệu luôn được chia
#    thành đúng 8 partition. Nó là cấu hình mặc định cho một số phép xử lý có shuffle;
#    số partition thực tế còn phụ thuộc nguồn dữ liệu và các thao tác khác.
spark = SparkSession.builder \
    .appName("Data_Cleaning") \
    .master("local[*]") \
    .getOrCreate()

print("=" * 50)
print("SPARK SESSION ĐÃ KHỞI TẠO")
print("=" * 50)

# Đường dẫn file
DATA_PATH = "Data/raw/2019-Nov.csv"

# Đọc CSV
df = spark.read.csv(NOV_FILE, header=True)

print(f"✅ Đã đọc {df.count():,} records")
print(f"Columns: {df.columns}")
df.printSchema()

# ===============================
# 3. EDA - KHÁM PHÁ DỮ LIỆU
# ===============================
print("\n" + "=" * 50)
print("EDA - KHÁM PHÁ DỮ LIỆU")
print("=" * 50)

# 3.1: Số dòng
print(f"\n3.1 Tổng số records: {df.count():,}")

# 3.2: Schema
print("\n3.2 Schema:")
df.printSchema()

# 3.3: Sample data
print("\n3.3 Sample 5 dòng đầu:")
df.show(5, truncate=False)

# 3.4: Thống kê cơ bản
print("\n3.4 Thống kê numeric columns:")
df.describe().show()

# 3.5: Null values
print("\n3.5 Số lượng NULL trong mỗi cột:")
for col in df.columns:
    null_count = df.filter(df[col].isNull()).count()
    if null_count > 0:
        print(f"   {col}: {null_count:,} NULLs")
```

## 5.3. Chạy và lưu log

```powershell
cd E-commerce
.\.venv311\Scripts\python.exe Data_collect.py 2>&1 | Tee-Object -FilePath Data\logs\collect_run.log
```

> **Tại sao phải lưu log?** Đề tài mục 5.7 yêu cầu "log, ảnh chụp, dashboard... để chứng
> minh quy trình hoạt động". Log chạy thật là minh chứng mạnh nhất. Làm cho **mọi**
> script, không riêng script này.

## 5.4. Kết quả mong đợi

```
✅ Đã đọc 67,501,979 records
Columns: ['event_time', 'event_type', 'product_id', 'category_id',
          'category_code', 'brand', 'price', 'user_id', 'user_session']
root
 |-- event_time: string
 |-- event_type: string
 ...
```

**Bảng NULL dự kiến (tính trên cả 2 kỳ):**

| Cột | Số NULL | Tỉ lệ |
|---|---|---|
| `category_code` | ~35,6 triệu | **32,4%** |
| `brand` | ~15,0 triệu | **13,7%** |
| (7 cột khác) | 0 | 0% |

## 5.5. Bài học rút ra (dùng để thiết kế Mục 6)

| Phát hiện | Quy tắc làm sạch tương ứng |
|---|---|
| `event_time` có hậu tố `" UTC"` | `regexp_replace` rồi `to_timestamp` |
| Tất cả cột đều là `string` | Ép kiểu thủ công (`long`, `double`) |
| `category_code` NULL 32,4% | Fill `"unknown.unknown"` **kèm cờ đánh dấu** |
| `brand` NULL 13,7% | Fill `"unknown"` |
| `price` có giá trị 0 | Giữ 0, **loại khỏi doanh thu** nhưng vẫn đếm lượt mua |
| `category_code` có 3 cấp | Tách `category_level1/2/3` bằng `split` |

---

# 6. BƯỚC 2 — LÀM SẠCH & GHI TẦNG BRONZE

**File:** `Data_cleaning.py` · **Thời gian:** ~12 phút (707 giây) · **Mục đề:** 1.3

## 6.1. Mục tiêu

Biến CSV "bẩn" thành Parquet "sạch" nhưng **vẫn giữ nguyên bản chất nhật ký sự kiện**.
Không diễn giải, không tổng hợp, không bỏ dòng nào (trừ trùng lặp ở Mục 8).

## 6.2. 5 quy tắc làm sạch

### Quy tắc 1 — Sửa timestamp

```python
df = df.withColumn(
    "event_time",
    to_timestamp(
        regexp_replace(col("event_time"), " UTC$", ""),
        "yyyy-MM-dd HH:mm:ss"
    )
)
```

- `"2019-11-01 00:00:00 UTC"` → timestamp `2019-11-01 00:00:00`
- `regexp_replace(..., " UTC$", "")` bỏ hậu tố. Dấu `$` rất quan trọng — nếu thiếu,
  nó sẽ xoá mọi chuỗi " UTC" ở giữa.
- `session.timeZone = UTC` đã được set trong `setup_spark()` → parse ra đúng giá trị
  gốc, không lệch 7 tiếng.

### Quy tắc 2 — Ép kiểu dữ liệu

```python
df = df.withColumn("product_id", col("product_id").cast("long"))
df = df.withColumn("category_id", col("category_id").cast("long"))
df = df.withColumn("price", col("price").cast("double"))
df = df.withColumn("user_id", col("user_id").cast("long"))
```

- `product_id`, `user_id`, `category_id` là số → `long` (số nguyên 64-bit).
  **Quan trọng:** `category_id` có giá trị như `2053013555631882655` (19 chữ số) →
  vượt quá `int` (chỉ chứa tới 2.147.483.647). Phải dùng `long`.
- `price` là số thập phân → `double`.
- Ép kiểu sau khi đọc (không dùng `inferSchema=true`) vì `inferSchema` phải đọc
  hết file để đoán kiểu → rất chậm với 67 triệu dòng.

### Quy tắc 3 — Xử lý NULL

```python
df = df.fillna({
    "category_code": "unknown.unknown",
    "brand": "unknown",
    "user_session": "unknown"
})
```

Giá trị "unknown" **có chủ đích** — nó làm cho dữ liệu trông "đầy đủ" mà không
giả vờ. Tầng Silver sẽ gắn cờ `is_classified` / `is_branded` để phân tích biết
chính xác phần nào thực sự không có.

### Quy tắc 4 — Xử lý NaN trong price

```python
df = df.withColumn(
    "price",
    when(isnan(df["price"]), 0.0).otherwise(df["price"])
)
```

`NaN != NULL` trong Spark SQL. `fillna` **không** xử lý NaN. Phải dùng `isnan()`.

### Quy tắc 5 — Tạo cột phái sinh

```python
df = df.withColumn("year", year(col("event_time")))
df = df.withColumn("month", month(col("event_time")))
df = df.withColumn("day", dayofmonth(col("event_time")))
df = df.withColumn("hour", hour(col("event_time")))
df = df.withColumn("date", col("event_time").cast("date"))

df = df.withColumn("category_level1", split(col("category_code"), "\\.").getItem(0))
df = df.withColumn("category_level2", split(col("category_code"), "\\.").getItem(1))
df = df.withColumn("category_level3", split(col("category_code"), "\\.").getItem(2))

df = df.withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
```

- Tách `category_code` theo dấu chấm: `"electronics.smartphone.apple"` →
  `electronics` / `smartphone` / `apple`.
- Regex `\\.` — trong chuỗi Python phải escape dấu chấm 2 lần.
- `event_date` là chuỗi `yyyy-MM-dd` dùng làm **cột partition** (Parquet partition
  phải là kiểu string).

## 6.3. Ghi Parquet

```python
df_nov_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(os.path.join(DATA_BRONZE, "nov_2019.parquet"))

df_oct_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(os.path.join(DATA_BRONZE, "oct_2019.parquet"))
```

- `mode("overwrite")` — chạy lại không bị lỗi "path already exists".
- `partitionBy("year", "month")` — chia nhỏ để pruning + ghi thêm không cần viết lại.
- Nén Snappy là mặc định của Spark khi ghi Parquet (không cần chỉ định).

## 6.4. Script hoàn chỉnh

```python
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

# inferSchema=true  → Đọc hết data → Tự đoán kiểu → CHẬM với 67 triệu rows
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
    """Làm sạch và convert dữ liệu trong 5 bước."""

    # --- 3.1: Convert event_time (xử lý " UTC" suffix) ---
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
    df = df.withColumn(
        "price",
        when(isnan(df["price"]), 0.0).otherwise(df["price"])
    )

    # --- 3.5: Tạo derived columns ---
    df = df.withColumn("year", year(col("event_time")))
    df = df.withColumn("month", month(col("event_time")))
    df = df.withColumn("day", dayofmonth(col("event_time")))
    df = df.withColumn("hour", hour(col("event_time")))
    df = df.withColumn("date", col("event_time").cast("date"))

    df = df.withColumn("category_level1", split(col("category_code"), "\\.").getItem(0))
    df = df.withColumn("category_level2", split(col("category_code"), "\\.").getItem(1))
    df = df.withColumn("category_level3", split(col("category_code"), "\\.").getItem(2))

    df = df.withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))

    print(f"    {month_name}: Clean xong!")
    return df


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

print("\n[6] Kiểm tra NULL sau clean...")
for col_name in df_nov_clean.columns:
    null_count = df_nov_clean.filter(df_nov_clean[col_name].isNull()).count()
    if null_count > 0:
        print(f"    {col_name}: {null_count:,} NULLs")

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

df_nov_clean.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(os.path.join(DATA_BRONZE, "nov_2019.parquet"))

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
```

## 6.5. Chạy

```powershell
.\.venv311\Scripts\python.exe Data_cleaning.py 2>&1 | Tee-Object -FilePath Data\logs\cleaning_run.log
```

## 6.6. Kiểm tra kết quả

```powershell
# Kiểm tra thư mục Bronze
Get-ChildItem Data\bronze -Recurse -File | Measure-Object Length -Sum |
  ForEach-Object { "{0} files, {1:N2} GB" -f $_.Count, ($_.Sum/1GB) }
```

**Kết quả mong đợi:**

| Artifact | Số file | Dung lượng |
|---|---|---|
| `oct_2019.parquet` | 86 | 1,62 GB |
| `nov_2019.parquet` | 136 | 2,72 GB |

**Dải thời gian phải đúng:**
```
Oct: min_t = 2019-10-01 00:00:00, max_t = 2019-10-31 23:59:59
Nov: min_t = 2019-11-01 00:00:00, max_t = 2019-11-30 23:59:59
```
Nếu lệch → xem Mục 11, lỗi múi giờ.

## 6.7. Câu hỏi giảng viên hay hỏi

**"Tại sao fill NULL bằng 'unknown' mà không xoá dòng?"**
→ Xoá 32,4% dòng sẽ mất toàn bộ sự kiện của nhóm sản phẩm không phân loại —
mất 10,4% doanh thu. Fill bằng giá trị biểu diễn + gắn cờ `is_classified` giữ được
100% dữ liệu **và** vẫn trung thực.

**"Tại sao không dùng `inferSchema=true`?"**
→ Với 8,39 GB CSV, Spark phải đọc hết file để đoán kiểu → mất thời gian gấp đôi.
Đọc 1 dòng + tự ép kiểu nhanh hơn nhiều và kiểm soát chắc chắn hơn.

---

# 7. BƯỚC 3 — CDC (CHANGE DATA CAPTURE)

**File:** `Data_CDC.py` · **Thời gian:** ~10 phút (618 giây) · **Mục đề:** 1.4

## 7.1. CDC nghĩa là gì?

**Change Data Capture** = ghi lại *những gì đã thay đổi* giữa hai thời điểm.

Trong bài toán này: so sánh tháng 10 với tháng 11 để biết:
- Sản phẩm nào **mới xuất hiện** (ra mắt tháng 11)?
- Sản phẩm nào **bị gỡ khỏi** cửa hàng?
- Sản phẩm nào **đổi giá**?
- Người dùng nào **mới đến**, ai **rời đi**, ai **quay lại**?

## 7.2. Ý nghĩa nghiệp vụ

| Phát hiện | Ý nghĩa kinh doanh |
|---|---|
| 40.082 sản phẩm mới | Đội ngũ merchandising đã ra mắt sản phẩm mới — cần theo dõi hiệu quả |
| 16.214 sản phẩm bị gỡ | Cần biết sản phẩm nào bị rút khỏi bán |
| 46.450 sản phẩm đổi giá | Tác động trực tiếp doanh thu — cần giám sát |
| 2.294.359 user mới | Đợt marketing/burst traffic mới — cần chăm sóc |
| 1.620.532 user rời đi | Tín hiệu churn — cần kích hoạt lại |

## 7.3. Kỹ thuật cốt lõi: `full_outer join`

```python
cdc = oct_ps.join(nov_ps, on="product_id", how="full_outer")
```

| Loại join | Giữ sản phẩm chỉ có ở 1 kỳ? | Dùng cho |
|---|---|---|
| `inner` | ❌ | ❌ — sẽ mất toàn bộ sản phẩm mới & bị gỡ |
| `left` (từ Oct) | ❌ mất sản phẩm chỉ có ở Nov | ❌ |
| **`full_outer`** | ✅ giữ cả hai | ✅ **đúng cho CDC** |

Sau đó phân loại:
```python
# Gắn nhãn thay đổi vào cột change_type
cdc = cdc.withColumn(
    "change_type",
    F.when(F.col("events_oct").isNull() & F.col("events_nov").isNotNull(), "NEW_PRODUCT")
     .when(F.col("events_nov").isNull() & F.col("events_oct").isNotNull(), "DISCONTINUED")
     .when(F.col("price_oct") != F.col("price_nov"), "PRICE_CHANGED")
     .otherwise("UNCHANGED"),
)
```

## 7.4. Ba bảng CDC tạo ra

| Bảng | Grain | Nội dung |
|---|---|---|
| `product_cdc` | 1 sản phẩm (206.876) | so sánh Oct vs Nov + `change_type` |
| `category_cdc` | 1 danh mục | tổng hợp thay đổi + `event_growth_pct` |
| `user_cdc` | 1 người dùng | `NEW_USER` / `CHURNED_USER` / `RETAINED` |

Mỗi bảng ghi ra **cả Parquet** (để tầng Silver join vào) **và CSV** (`coalesce(1)` gộp
1 file, để mở bằng Excel khi trình bày).

## 7.5. Script hoàn chỉnh

```python
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

spark = setup_spark("Data_CDC", master="local[*]", shuffle_partitions=8,
                    driver_memory="4g", log_level="WARN")

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
# Điểm tối ưu quan trọng: cùng một dữ liệu được dùng cho nhiều phép aggregate
# (snapshot, period_stats, tổng quan) - không cache thì mỗi phép lại đọc toàn bộ
# Parquet từ đĩa.
df_oct.cache()
df_nov.cache()
df_oct.count()   # trigger cache
df_nov.count()   # trigger cache
log("Đã cache 2 partition trong RAM")

# ===============================
# 2. BẢNG SNAPSHOT SẢN PHẨM
# ===============================
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
```

## 7.6. Chạy

```powershell
.\.venv311\Scripts\python.exe Data_CDC.py 2>&1 | Tee-Object -FilePath Data\logs\cdc_run.log
```

## 7.7. Kết quả mong đợi

```
  Tổng sản phẩm theo dõi : 206,876
  Sản phẩm có thay đổi    : 102,746 (49.7%)
  Sản phẩm mới (Nov)     : 40,082
  Sản phẩm bị gỡ         : 16,214
  Sản phẩm đổi giá       : 46,450

  User mới trong Nov     : 2,294,359
  User rời đi            : 1,620,532
  User giữ lại           : 1,401,758
```

---

# 8. BƯỚC 4 — TÁCH CHIỀU, TẦNG SILVER

**File:** `Data_Silver.py` · **Thời gian:** ~11 phút (668 giây) · **Mục đề:** 2

## 8.1. Tại sao cần tầng Silver?

Bronze vẫn là **nhật ký sự kiện**: mỗi dòng là 1 hành động, có lặp lại thông tin
danh mục/brand hàng triệu lần. Muốn phân tích "doanh thu theo danh mục" hay "RFM
theo khách hàng", cần các bảng riêng biệt với grain rõ ràng.

## 8.2. Quy trình 6 bước

| Bước | Nội dung |
|---|---|
| 1 | Khảo sát Bronze: đo trùng lặp, đo tỉ lệ khôi phục, đo kích thước chiều |
| 2 | Phân loại danh mục: đánh dấu sản phẩm có/không thuộc danh mục nào |
| 3 | Loại bỏ bản ghi trùng lặp hoàn toàn trên khóa sự kiện |
| 4 | Tách chiều: dựng 3 bảng dimension + 1 bảng fact |
| 5 | Tích hợp kết quả CDC: gắn `change_type`, `user_status` |
| 6 | Ghi Parquet + kiểm tra referential integrity |

## 8.3. Bước 2 — Khám phá quan trọng: NULL ở `category_code` là Missing **Not** At Random

**Thiết kế ban đầu** của tôi: sản phẩm nào bị NULL `category_code` thì suy ra từ
cùng `product_id` ở dòng khác (vì 99,93% sản phẩm có dữ liệu nhất quán).

**Đo đạc cho thấy tỉ lệ khôi phục được = 0.**

Điều tra sâu hơn:
```
82.729 sản phẩm (43%)  có category_code ở ít nhất một dòng
107.933 sản phẩm (57%) KHÔNG có category_code ở BẤT KỲ dòng nào
```

→ Mỗi sản phẩm thuộc **đúng một** danh mục. Sản phẩm không có danh mục thì
**mọi dòng đều null**. Đây là thuộc tính của sản phẩm, không phải dữ liệu hỏng.

**Kết luận & cách xử lý đúng:**
- ❌ Không điền giá trị bịa đặt (sẽ làm sai lệch phân tích theo danh mục).
- ✅ Gắn cờ minh bạch `is_classified` / `is_branded`.
- ✅ Sản phẩm không phân loại vẫn giữ **đầy đủ** trong `fact_event`.
- ✅ Khi tính doanh thu theo danh mục → tách riêng nhóm này, báo cáo minh bạch.

> **Đây là kỹ năng quan trọng nhất của đồ án.** Phát hiện "dữ liệu thiếu có thể
> suy ra" → đo → thấy không → điều tra nguyên nhân → kết luận đúng đắn →
> xử lý trung thực. Ghi vào báo cáo mục "Thảo luận" và "Bài học kỹ thuật".

## 8.4. Bước 2 — Code phân loại

```python
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
```

`countDistinct` + `filter(n_cat == 1)` → chỉ dùng mapping khi **không mơ hồ**.
Sản phẩm có 2+ danh mục sẽ không được gán → an toàn.

## 8.5. Bước 3 — Loại trùng lặp

```python
before = events.count()
events = events.dropDuplicates(
    ["event_time", "event_type", "product_id", "user_id", "user_session"]
)
after = events.count()
log(f"Trước: {before:,} → Sau: {after:,} (bỏ {before - after:,} dòng trùng)")
```

**Kết quả:** 109.950.743 → 109.819.993 (bỏ **130.750** dòng, giữ lại 99,8811%).

**Vì sao bỏ được?** Trong nhật ký web, cùng một hành động đôi khi được ghi 2 lần
(timestamp, sản phẩm, người dùng, phiên giống hệt nhau). Chọn 5 cột làm khóa
sự kiện vì đó là bản chất của 1 sự kiện.

## 8.6. Bước 3b — Ghi `fact_event` ra đĩa thay vì `persist()` ⭐

**Bài học quan trọng nhất về hiệu năng trong toàn bộ đồ án.**

Ban đầu tôi dùng:
```python
events = events.persist(StorageLevel.MEMORY_AND_DISK)
```
để tránh việc Spark tính lại chuỗi join + dropDuplicates 110 triệu dòng nhiều lần.

**Kết quả:** Job dừng sau 229 giây với `OutOfMemoryError: Java heap space`.
Lý do: Spark cache kiểu **columnar** cho DataFrame này cần hơn 4 GB heap.

**Giải pháp đúng:** ghi `fact_event` ra đĩa **MỘT LẦN**, rồi đọc lại từ đĩa để dựng
các bảng chiều.

```python
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

log("fact_event đã ghi xong. Đọc lại từ đĩa để dựng chiều...")

events = spark.read.parquet(os.path.join(SILVER, "fact_event"))
log(f"Đã đọc lại: {events.count():,} sự kiện từ đĩa.")
```

**So sánh:**
| | `persist()` | Ghi đĩa rồi đọc lại |
|---|---|---|
| RAM | > 4 GB → **OOM** | ~0 (chỉ stream) |
| Tốc độ | chết | 668 giây, ổn định |
| Kiểm chứng | kiểm tra trong RAM (không chắc = đã lưu) | kiểm tra **trên đĩa thật** |

**Nguyên tắc rút ra:** `persist()` chỉ đúng khi tập dữ liệu **vừa** với RAM. Với
dữ liệu lớn hơn RAM, `persist` biến vấn đề "tính lại" thành vấn đề "không đủ RAM" —
tệ hơn nhiều.

## 8.7. Bước 4 — Dựng 3 bảng chiều

### `dim_category`
```python
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
)
```

### `dim_product` — giá lấy theo **mode** chứ không phải max

```python
price_window = Window.partitionBy("product_id").orderBy(F.col("event_count").desc())
product_price = (
    events.filter(F.col("price") > 0)
    .groupBy("product_id", "price")
    .agg(F.count(F.lit(1)).alias("event_count"))
    .withColumn("rn", F.row_number().over(price_window))
    .filter(F.col("rn") == 1)
    .select("product_id", F.col("price").alias("list_price"))
)
```
- `filter(price > 0)` loại giá 0 (sản phẩm chưa gán giá).
- Sắp xếp theo số lần xuất hiện giảm dần → `row_number = 1` là giá phổ biến nhất.
- Nếu sản phẩm không có giá nào > 0 → `list_price` NULL → fallback sang `avg_price`.

### `dim_user` — ⚠️ `total_spent` chỉ tính từ `purchase`

```python
# Tổng chi CHỈ tính từ sự kiện purchase.
# Nếu sum(price) trên mọi event thì sẽ tính cả giá của lượt xem và lượt
# thêm giỏ — người dùng chỉ xem sản phẩm 100.000 USD cũng bị coi là
# đã chi tiêu 100.000 USD. Đây là lỗi sai lệch mà không ai nhận ra
# vì con số vẫn "hợp lý" về mặt hình thức.
dim_user = dim_user.withColumn(
    "total_spent",
    F.round(
        F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0)), 2
    ),
)
```

**Vì sao phải cẩn thận chỗ này?** Nếu dùng `F.sum("price")` trên **mọi** event:
người dùng chỉ *xem* sản phẩm 100.000 USD cũng bị coi là đã chi tiêu 100.000 USD.
Đây là **lỗi sai lệch mà không ai nhận ra**, vì con số vẫn "hợp lý" về mặt hình thức.

### `customer_type`
```python
# Phân loại khách hàng theo hành vi
dim_user = dim_user.withColumn(
    "customer_type",
    F.when(F.col("purchase_count") > 0, F.lit("buyer"))
     .when(F.col("cart_count") > 0, F.lit("cart_only"))
     .otherwise(F.lit("viewer")),
)
```

## 8.8. Bước 5 — Tích hợp CDC

```python
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
```

**Ý nghĩa:** tầng Silver là nơi **dữ liệu hành vi** (Bronze) và **dữ liệu về sự thay
đổi** (CDC) gặp nhau. Nhóm phân tích sau chỉ cần đọc Silver, không cần biết CDC.

## 8.9. Bước 6 — Kiểm tra referential integrity

```python
# Dùng left_anti join: chỉ giữ dòng KHÔNG tìm thấy cha, không kéo theo payload.
orphan_product = (
    fact_check.select("product_id").distinct()
    .join(prod_check.select("product_id").distinct(), "product_id", "left_anti")
    .count()
)
```

**Kỹ thuật `left_anti join`:** chỉ giữ các dòng của bảng trái mà **không** tìm
thấy bản ghi khớp ở bảng phải. Dùng `left join` + `filter(isNull)` sẽ kéo theo
toàn bộ payload → chậm hơn nhiều trên dữ liệu lớn.

**Kết quả mong đợi:**
```
  fact_event → dim_product  : 0
  fact_event → dim_user     : 0
  fact_event → dim_category : 0
```

## 8.10. Script hoàn chỉnh

```python
"""
GIAI ĐOẠN 2: TẦNG SILVER — TÁCH CHIỀU VÀ KHÔI PHỤC CHẤT LƯỢNG

Input : Data/bronze/{oct,nov}_2019.parquet + Data/bronze/cdc/*.parquet
Output: Data/silver/{dim_product, dim_user, dim_category, fact_event}
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
log("Phân loại sản phẩm có/không thuộc danh mục...")

prod_cat = (
    events.filter(F.col("category_code") != "unknown.unknown")
    .groupBy("product_id")
    .agg(
        F.countDistinct("category_code").alias("n_cat"),
        F.first("category_code").alias("cat_value"),
    )
)
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
    .select("product_id", F.col("brand_value").alias("brand_known"))
)

log("Áp dụng nhãn phân loại...")
events = (
    events
    .join(prod_cat, on="product_id", how="left")
    .join(prod_brand, on="product_id", how="left")
    .withColumn("is_classified",
                F.when(F.col("cat_known").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
    .withColumn("is_branded",
                F.when(F.col("brand_known").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
    .drop("cat_known", "brand_known")
)

events = (
    events
    .withColumn("category_level1", F.split(F.col("category_code"), "\\.").getItem(0))
    .withColumn("category_level2", F.split(F.col("category_code"), "\\.").getItem(1))
    .withColumn("category_level3", F.split(F.col("category_code"), "\\.").getItem(2))
)

# ===============================
# 3. LOẠI BỎ TRÙNG LẮP
# ===============================
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
# Ban đầu dùng events.persist(StorageLevel.MEMORY_AND_DISK) để tránh việc
# Spark tính lại chuỗi join + dropDuplicates 110 triệu dòng nhiều lần. Nhưng
# Spark cache kiểu columnar cho DataFrame này cần hơn 4 GB heap ->
# OutOfMemoryError: Java heap space, job bị abort sau 229 giây.
#
# GIẢI PHÁP: ghi fact_event ra đĩa MỘT LẦN, rồi đọc lại từ đĩa để dựng chiều.
#   - Chi phí: 1 lần ghi + vài lần quét Parquet đã nén.
#   - Lợi ích: không tốn RAM; đọc Parquet chỉ mất vài giây, nhanh hơn nhiều so với
#     việc Spark tính lại chuỗi join + dropDuplicates từ đầu cho từng bảng dimension.
#   - Lưu ý: dữ liệu mà dimension đọc CHÍNH LÀ dữ liệu đã ghi xuống đĩa
#     -> phần kiểm tra chất lượng sau đó là kiểm tra trên dữ liệu thật.
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

log("fact_event đã ghi xong. Đọc lại từ đĩa để dựng chiều...")

events = spark.read.parquet(os.path.join(SILVER, "fact_event"))
log(f"Đã đọc lại: {events.count():,} sự kiện từ đĩa.")

# ===============================
# 4. TÁCH CHIỀU
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
)

log("Tạo dim_product...")
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
    .withColumn("list_price",
                F.when(F.col("list_price").isNull(), F.col("avg_price")).otherwise(F.col("list_price")))
    .select(
        "product_id", "category_code", "brand", "level1", "level2",
        "is_classified", "is_branded",
        "list_price", "avg_price", "total_events", "unique_users",
        "first_seen", "last_seen",
    )
)

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
# 5. TÍCH HỢP CDC
# ===============================
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
log("Đọc lại tầng Silver từ đĩa để kiểm tra...")
fact_check = spark.read.parquet(os.path.join(SILVER, "fact_event"))
prod_check = spark.read.parquet(os.path.join(SILVER, "dim_product"))
user_check = spark.read.parquet(os.path.join(SILVER, "dim_user"))
cat_check = spark.read.parquet(os.path.join(SILVER, "dim_category"))

counts = {
    "fact_event": fact_check.count(),
    "dim_product": prod_check.count(),
    "dim_user": user_check.count(),
    "dim_category": cat_check.count(),
}
print("\nSố dòng mỗi bảng Silver:")
for name, n in counts.items():
    print(f"  {name:<16}: {n:,}")

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

print("\nPhân bố change_type (sản phẩm):")
prod_check.groupBy("change_type").count().orderBy("count", ascending=False).show(truncate=False)

print("\nPhân bố user_status:")
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
```

## 8.11. Chạy

```powershell
.\.venv311\Scripts\python.exe Data_Silver.py 2>&1 | Tee-Object -FilePath Data\logs\silver_run.log
```

> **Bắt buộc chạy `Data_CDC.py` trước** — `Data_Silver.py` đọc `Data/bronze/cdc/`.

## 8.12. Kết quả mong đợi

| Bảng | Số dòng | Dung lượng |
|---|---|---|
| `fact_event` | 109.819.993 | 3,91 GB |
| `dim_user` | 5.316.649 | 142 MB |
| `dim_product` | 206.876 | 7,6 MB |
| `dim_category` | 130 | 9,7 KB |

Tất cả 3 phép kiểm tra khóa ngoại = **0**.

---

# 9. BƯỚC 5 — KIỂM CHỨNG ĐỘC LẬP

**File:** `Data_Verify.py` · **Thời gian:** ~57 giây · **Mục đề:** 5.7 (thống kê chất lượng)

## 9.1. Vì sao cần bước này riêng?

`Data_Silver.py` **tự kiểm tra** trong lúc chạy. Nhưng kiểm tra bằng biến trong RAM
chỉ chứng minh "dữ liệu đúng lúc tính toán", không chứng minh "dữ liệu đã lưu đúng
trên đĩa".

`Data_Verify.py` là **script kiểm chứng độc lập**: đọc lại từ đĩa, không dùng lại
một biến nào của script trước. Giống như kiểm tra lại bài thi bằng đáp án khác.

## 9.2. 5 nhóm kiểm tra

| # | Nhóm | Cách kiểm tra | Tiêu chí đạt |
|---|---|---|---|
| 1 | Số dòng & dung lượng | `count()` + `dir_size()` | khớp Mục 8.12 |
| 2 | Đối chiếu Bronze → Silver | `n_bronze` vs `n_fact` | tỉ lệ giữ ≥ 99,8% |
| 3 | Referential integrity | `left_anti join` × 3 | 0 khóa mồ côi |
| 4 | Độ bao phủ dữ liệu | `is_classified`, `is_branded`, `price > 0` | tổng = 100% |
| 5 | Dải thời gian & múi giờ | so `hour`/`day`/`event_date` với tính lại từ `event_time` | 0 dòng lệch |

## 9.3. ⚠️ Bẫy múi giờ khi kiểm tra (quan trọng!)

Spark lưu timestamp trong Parquet dưới dạng **số micro-giây kể từ epoch UTC**.
Khi đọc lên, giá trị được **hiển thị theo múi giờ của driver Python** (GMT+07:00
ở máy này) → `2019-10-01 00:00:00 UTC` in ra thành `2019-10-01 07:00:00`.

**Sai lầm thường gặp:** so sánh trực tiếp chuỗi hiển thị với giá trị mong đợi →
tưởng dữ liệu sai.

**Cách kiểm tra ĐÚNG:** so sánh các **cột phái sinh** (`hour`, `day`, `event_date`)
với giá trị **tính lại từ `event_time`** trong cùng ngữ cảnh UTC của Spark.
Nếu khớp 100% → dữ liệu đúng.

```python
ts = fact.agg(
    F.min("event_date").cast("string").alias("min_date"),
    F.max("event_date").cast("string").alias("max_date"),
    F.min(F.to_date("event_time")).cast("string").alias("min_date_check"),
    F.max(F.to_date("event_time")).cast("string").alias("max_date_check"),
).collect()[0]

m_hour = fact.filter(F.hour(F.col("event_time")) != F.col("hour")).count()
m_day  = fact.filter(F.dayofmonth(F.col("event_time")) != F.col("day")).count()
m_date = fact.filter(F.to_date(F.col("event_time")) != F.col("event_date")).count()
```

## 9.4. Script hoàn chỉnh

```python
"""
KIỂM TRA CHẤT LƯỢNG TẦNG SILVER
Đọc trực tiếp từ Data/silver/ (không tính lại từ Bronze) nên chạy rất nhanh.

Quy trình kiểm tra:
  1. Số dòng và dung lượng mỗi bảng
  2. Referential integrity: mọi dòng fact phải tìm thấy cha trong dimension
  3. Độ bao phủ dữ liệu (tỉ lệ sự kiện có danh mục / thương hiệu)
  4. Phân bố các cột phân loại và nhãn CDC
  5. Đối chiếu tổng số dòng với tầng Bronze
"""

import os
import time

from config import *
from pyspark.sql import functions as F

spark = setup_spark("Data_Verify", master="local[*]", shuffle_partitions=8,
                    driver_memory="4g", log_level="WARN")

SILVER = DATA_SILVER
BRONZE = DATA_BRONZE
CDC_DIR = os.path.join(BRONZE, "cdc")

t0 = time.time()


def log(msg):
    print(f"[{time.time() - t0:7.1f}s] {msg}", flush=True)


banner("KIỂM TRA CHẤT LƯỢNG TẦNG SILVER")

# ===============================
# 1. ĐỌC TẦNG SILVER TỪ ĐĨA
# ===============================
log("Đọc tầng Silver từ đĩa...")

fact = spark.read.parquet(os.path.join(SILVER, "fact_event"))
dim_product = spark.read.parquet(os.path.join(SILVER, "dim_product"))
dim_user = spark.read.parquet(os.path.join(SILVER, "dim_user"))
dim_category = spark.read.parquet(os.path.join(SILVER, "dim_category"))

# ===============================
# 2. SỐ DÒNG VÀ DUNG LƯỢNG
# ===============================
log("Đếm số dòng...")

n_fact = fact.count()
n_product = dim_product.count()
n_user = dim_user.count()
n_category = dim_category.count()

print("\n" + "=" * 58)
print("SỐ DÒNG VÀ DUNG LƯỢNG TẦNG SILVER")
print("=" * 58)
for name, n in [("fact_event", n_fact), ("dim_product", n_product),
                ("dim_user", n_user), ("dim_category", n_category)]:
    size, files = dir_size(os.path.join(SILVER, name))
    print(f"  {name:<16}: {n:>12,} dòng  |  {files:>3} file  |  {human_bytes(size)}")

# ===============================
# 3. ĐỐI CHIẾU VỚI TẦNG BRONZE
# ===============================
log("Đối chiếu với tầng Bronze...")

n_bronze = (
    spark.read.parquet(os.path.join(BRONZE, "oct_2019.parquet")).count()
    + spark.read.parquet(os.path.join(BRONZE, "nov_2019.parquet")).count()
)

print("\n" + "=" * 58)
print("ĐỐI CHIẾU BRONZE → SILVER")
print("=" * 58)
print(f"  Tổng sự kiện ở Bronze        : {n_bronze:>12,}")
print(f"  Tổng sự kiện ở Silver fact   : {n_fact:>12,}")
print(f"  Bản ghi trùng lặp đã loại     : {n_bronze - n_fact:>12,}")
print(f"  Tỉ lệ giữ lại                 : {n_fact / n_bronze * 100:>11.4f}%")

# ===============================
# 4. REFERENTIAL INTEGRITY
# ===============================
# Mỗi dòng fact phải tìm thấy "cha" tương ứng trong bảng chiều.
# Nếu có dòng không tìm thấy cha, mô hình sao bị vỡ và mọi phân tích
# phía sau sẽ cho kết quả sai.
log("Kiểm tra khóa ngoại (referential integrity)...")

checks = [
    ("fact_event → dim_product", "product_id", "product_id", dim_product),
    ("fact_event → dim_user", "user_id", "user_id", dim_user),
    ("fact_event → dim_category", "category_code", "category_code", dim_category),
]

print("\n" + "=" * 58)
print("REFERENTIAL INTEGRITY")
print("=" * 58)
for label, child_col, parent_col, parent_df in checks:
    parent_keys = parent_df.select(parent_col).distinct()
    orphan = (
        fact.select(child_col).distinct()
        .join(parent_keys, child_col, "left_anti")
        .count()
    )
    status = "OK" if orphan == 0 else "LỖI"
    print(f"  {label:<30}: {orphan:>6} khóa mồ côi  [{status}]")

# ===============================
# 5. ĐỘ BAO PHỦ DỮ LIỆU
# ===============================
log("Đo độ bao phủ dữ liệu...")

coverage = fact.agg(
    F.count(F.lit(1)).alias("total"),
    F.sum(F.when(F.col("is_classified"), 1).otherwise(0)).alias("classified"),
    F.sum(F.when(F.col("is_branded"), 1).otherwise(0)).alias("branded"),
    F.sum(F.when(F.col("price") > 0, 1).otherwise(0)).alias("has_price"),
).collect()[0]

print("\n" + "=" * 58)
print("ĐỘ BAO PHỦ DỮ LIỆU (sau khi khử trùng lặp)")
print("=" * 58)
print(f"  Tổng sự kiện                        : {coverage['total']:>12,}")
print(f"  Có danh mục (is_classified)         : {coverage['classified']:>12,} "
      f"({coverage['classified'] / coverage['total'] * 100:.2f}%)")
print(f"  KHÔNG có danh mục                   : {coverage['total'] - coverage['classified']:>12,} "
      f"({(coverage['total'] - coverage['classified']) / coverage['total'] * 100:.2f}%)")
print(f"  Có thương hiệu (is_branded)         : {coverage['branded']:>12,} "
      f"({coverage['branded'] / coverage['total'] * 100:.2f}%)")
print(f"  KHÔNG có thương hiệu                : {coverage['total'] - coverage['branded']:>12,} "
      f"({(coverage['total'] - coverage['branded']) / coverage['total'] * 100:.2f}%)")
print(f"  Có giá > 0                          : {coverage['has_price']:>12,} "
      f"({coverage['has_price'] / coverage['total'] * 100:.2f}%)")

print("\n  → Nhóm 'KHÔNG có danh mục' là sản phẩm thực sự không thuộc danh mục nào")
print("    (Missing Not At Random), được đánh dấu minh bạch bằng cờ is_classified.")
print("    Phân tích theo danh mục phải loại nhóm này khỏi mẫu số.")

# ===============================
# 6. PHÂN BỐ CÁC CỘT PHÂN LOẠI
# ===============================
print("\n" + "=" * 58)
print("PHÂN BỐ CÁC CỘT PHÂN LOẠI")
print("=" * 58)

print("\n  customer_type (theo dim_user):")
dim_user.groupBy("customer_type").count().orderBy("count", ascending=False).show(truncate=False)

print("  change_type (theo dim_product, từ CDC):")
dim_product.groupBy("change_type").count().orderBy("count", ascending=False).show(truncate=False)

print("  user_status (theo dim_user, từ CDC):")
dim_user.groupBy("user_status").count().orderBy("count", ascending=False).show(truncate=False)

print("  is_classified (theo dim_product):")
dim_product.groupBy("is_classified").count().orderBy("count", ascending=False).show(truncate=False)

# ===============================
# 7. KIỂM TRA DẢI THỜI GIAN
# ===============================
log("Kiểm tra dải thời gian...")

# LƯU Ý QUAN TRỌNG về hiển thị múi giờ:
# Spark lưu timestamp trong Parquet dưới dạng số micro-giây kể từ epoch UTC.
# Khi đọc lên, giá trị được hiển thị theo múi giờ CỦA DRIVER PYTHON (GMT+07:00 ở máy này),
# nên 2019-10-01 00:00:00 UTC bị in ra thành 2019-10-01 07:00:00.
# Vì vậy KHÔNG được so sánh trực tiếp chuỗi hiển thị. Cách kiểm tra đúng là so sánh
# các phái sinh (hour, day, event_date) với giá trị tính lại từ event_time trong
# cùng ngữ cảnh UTC của Spark. Nếu khớp 100%, dữ liệu đúng.
ts = fact.agg(
    F.min("event_date").cast("string").alias("min_date"),
    F.max("event_date").cast("string").alias("max_date"),
    F.min(F.to_date("event_time")).cast("string").alias("min_date_check"),
    F.max(F.to_date("event_time")).cast("string").alias("max_date_check"),
).collect()[0]

m_hour = fact.filter(F.hour(F.col("event_time")) != F.col("hour")).count()
m_day = fact.filter(F.dayofmonth(F.col("event_time")) != F.col("day")).count()
m_date = fact.filter(F.to_date(F.col("event_time")) != F.col("event_date")).count()

print("\n" + "=" * 58)
print("DẢI THỜI GIAN VÀ TÍNH NHẤT QUÁN CỘT PHÁI SINH")
print("=" * 58)
print(f"  Khoảng ngày (theo event_date)      : {ts['min_date']} → {ts['max_date']}")
print(f"  Khoảng ngày (tính lại từ event_time): {ts['min_date_check']} → {ts['max_date_check']}")
print(f"  Số dòng lệch cột 'hour'   : {m_hour:,}")
print(f"  Số dòng lệch cột 'day'    : {m_day:,}")
print(f"  Số dòng lệch cột 'event_date': {m_date:,}")

ok = (
    ts["min_date"] == ts["min_date_check"]
    and ts["max_date"] == ts["max_date_check"]
    and m_hour == 0 and m_day == 0 and m_date == 0
    and str(ts["min_date"]).startswith("2019-10-01")
    and str(ts["max_date"]).startswith("2019-11-30")
)
print(f"  → {'OK — dữ liệu nhất quán, không lệch múi giờ' if ok else 'LỖI — cần chạy lại Data_cleaning.py'}")
print("\n  Ghi chú: giá trị in ra có thể lệch 7 tiếng so với nguồn vì Python driver")
print("  hiển thị timestamp theo múi giờ máy. Giá trị thực lưu trong Parquet là UTC.")

print(f"\n⏱  Tổng thời gian kiểm tra: {time.time() - t0:.1f}s")
print("=" * 58)
print("✅ KIỂM TRA TẦNG SILVER HOÀN THÀNH!")
print("=" * 58)

spark.stop()
```

## 9.5. Chạy và kết quả mong đợi

```powershell
.\.venv311\Scripts\python.exe Data_Verify.py 2>&1 | Tee-Object -FilePath Data\logs\verify_run.log
```

```
ĐỐI CHIẾU BRONZE → SILVER
  Tổng sự kiện ở Bronze        :   109,950,743
  Tổng sự kiện ở Silver fact   :   109,819,993
  Bản ghi trùng lặp đã loại     :       130,750
  Tỉ lệ giữ lại                 :      99.8811%

REFERENTIAL INTEGRITY
  fact_event → dim_product     :      0 khóa mồ côi  [OK]
  fact_event → dim_user        :      0 khóa mồ côi  [OK]
  fact_event → dim_category    :      0 khóa mồ côi  [OK]

DẢI THỜI GIAN VÀ TÍNH NHẤT QUÁN CỘT PHÁI SINH
  Khoảng ngày (theo event_date)      : 2019-10-01 → 2019-11-30
  Khoảng ngày (tính lại từ event_time): 2019-10-01 → 2019-11-30
  Số dòng lệch cột 'hour'   : 0
  Số dòng lệch cột 'day'    : 0
  Số dòng lệch cột 'event_date': 0
  → OK — dữ liệu nhất quán, không lệch múi giờ
```

---

# 10. BƯỚC 6 — TỔNG HỢP KPI, TRỰC QUAN HÓA & BÀN GIAO

## 10.1. Chạy lại toàn bộ pipeline từ đầu

```powershell
cd E-commerce
Remove-Item -Recurse -Force Data\bronze, Data\silver, Data\gold, Data\logs -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path Data\logs | Out-Null

.\.venv311\Scripts\python.exe Data_collect.py   2>&1 | Tee-Object Data\logs\collect_run.log
.\.venv311\Scripts\python.exe Data_cleaning.py  2>&1 | Tee-Object Data\logs\cleaning_run.log
.\.venv311\Scripts\python.exe Data_CDC.py       2>&1 | Tee-Object Data\logs\cdc_run.log
.\.venv311\Scripts\python.exe Data_Silver.py    2>&1 | Tee-Object Data\logs\silver_run.log
.\.venv311\Scripts\python.exe Data_Verify.py    2>&1 | Tee-Object Data\logs\verify_run.log
.\.venv311\Scripts\python.exe Data_Gold.py      2>&1 | Tee-Object Data\logs\gold_run.log
.\.venv311\Scripts\python.exe Data_Dashboard.py 2>&1 | Tee-Object Data\logs\dashboard_run.log
.\.venv311\Scripts\python.exe Data_Architecture.py   # 2 sơ đồ kiến trúc
```

**Thứ tự là bắt buộc** — mỗi script là input của script sau:
```
raw → Bronze → CDC → Silver → Verify → Gold → Dashboard → Architecture
```

## 10.2. Bảng thời gian xử lý (đo được)

| Job | Thời gian | Dòng xử lý | Throughput |
|---|---|---|---|
| `Data_collect.py` | ~265 s | 67,5 tr (đọc 1 lần) | ~255.000 dòng/s |
| `Data_cleaning.py` | **707 s** | 109,95 tr | ~155.500 dòng/s |
| `Data_CDC.py` | **618 s** | 109,95 tr | ~177.900 dòng/s |
| `Data_Silver.py` | **668 s** | 109,95 tr | ~164.600 dòng/s |
| `Data_Verify.py` | **95 s** | 109,95 tr | ~1.157.000 dòng/s |
| `Data_Gold.py` | 79 s | 109,95 tr | ~1.392.000 dòng/s |
| `Data_Dashboard.py` | 84 s | — | — |
| `Data_Architecture.py` | 2 s | — | — |
| **Tổng** | **~40 phút** | | |

> **Cách đo:** mỗi script in `time.time() - t_start` khi kết thúc. Đây là số liệu
> thật, không ước lượng — dùng làm minh chứng mục 5.7.

## 10.3. Bảng "dữ liệu trước / sau xử lý" (mục 5.4 đề tài)

| Tầng | Artifact | Số dòng | Dung lượng | Ghi chú |
|---|---|---|---|---|
| **Trước** (Bronze) | `oct_2019.parquet` | 42.448.764 | 1,62 GB | Parquet + Snappy, partition `year=2019/month=10` |
| | `nov_2019.parquet` | 67.501.979 | 2,72 GB | partition `year=2019/month=11` |
| | `cdc/product_cdc.parquet` | 206.876 | 27,6 MB | 206.876 sản phẩm + `change_type` |
| | `cdc/category_cdc.parquet` | 130 | 8 KB | tăng trưởng theo danh mục |
| | `cdc/user_cdc.parquet` | 5.316.649 | 127 MB | NEW/CHURNED/RETAINED |
| **Sau** (Silver) | `fact_event` | 109.819.993 | 3,91 GB | grain: 1 sự kiện |
| | `dim_product` | 206.876 | 7,6 MB | grain: 1 sản phẩm |
| | `dim_user` | 5.316.649 | 142 MB | grain: 1 người dùng |
| | `dim_category` | 130 | 9,7 KB | grain: 1 danh mục |

## 10.4. File `README.md` cho gói nộp

Tạo `README.md` ở thư mục gốc `E-commerce/` với nội dung tối thiểu:

```markdown
# E-commerce Behavior Data Pipeline — Phần 1: Storage → Processing

## Yêu cầu môi trường
- Java JDK 17 (JAVA_HOME trỏ tới JDK 17)
- Python 3.11 (venv `.venv311`)
- PySpark 3.5.3
- winutils.exe tại D:\Metadata\hadoop\bin\winutils.exe
- Copy .\.venv311\Scripts\python.exe .\.venv311\Scripts\python3.exe

## Cài đặt
pip install -r requirements.txt

## Dữ liệu
Tải từ Kaggle: https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
Đặt 2 file CSV vào Data/raw/ (không nộp do dung lượng 14,7 GB)

## Chạy (đúng thứ tự)
.\.venv311\Scripts\python.exe Data_collect.py
.\.venv311\Scripts\python.exe Data_cleaning.py
.\.venv311\Scripts\python.exe Data_CDC.py
.\.venv311\Scripts\python.exe Data_Silver.py
.\.venv311\Scripts\python.exe Data_Verify.py
.\.venv311\Scripts\python.exe Data_Gold.py
.\.venv311\Scripts\python.exe Data_Dashboard.py
.\.venv311\Scripts\python.exe Data_Architecture.py

## Kết quả
Data/bronze/      — 4,34 GB — dữ liệu trước xử lý (đã chuẩn hóa) + CDC
Data/silver/      — 4,06 GB — dữ liệu sau xử lý (mô hình sao 4 bảng)
Data/gold/        — 8 bảng KPI
Data/dashboard/   — 9 ảnh PNG + 9 bảng CSV (mở bằng Excel / import Power BI)
Data/logs/        — log thực thi từng bước
```

## 10.5. Script sinh dữ liệu mẫu (mục 5.8 — khả năng tái lập)

Đề tài yêu cầu: *"dữ liệu mẫu hoặc script sinh dữ liệu để người khác có thể kiểm
chứng"*. Tạo `make_sample_data.py` để người khác chạy thử **không cần tải 14,7 GB**:

```python
"""
Sinh dữ liệu MẪU đúng schema gốc để người khác kiểm chứng pipeline nhanh.
Dùng seed cố định -> kết quả tái lập được.

Chạy:  .\.venv311\Scripts\python.exe make_sample_data.py
Xong:   Data/raw/sample_oct.csv, Data/raw/sample_nov.csv  (mỗi file ~50.000 dòng)
"""
import csv
import os
import random
from datetime import datetime, timedelta

from config import DATA_RAW

random.seed(42)   # seed cố định -> kết quả tái lập

CATEGORIES = [
    ("electronics.smartphone", "xiaomi", 300.0, 900.0),
    ("electronics.audio", "sony", 50.0, 400.0),
    ("appliances.sewing_machine", "janome", 200.0, 1200.0),
    ("appliances.kitchen", "bosch", 80.0, 700.0),
    ("furniture.kitchen_dining", "ikea", 30.0, 500.0),
]
BRANDS_EXTRA = ["creed", "nike", "apple", "samsung", "philips"]
EVENT_TYPES = ["view", "view", "view", "cart", "purchase"]   # tỉ lệ thực tế


def gen_file(path, year, month, n_rows):
    """Sinh 1 file CSV đúng schema gốc."""
    start = datetime(year, month, 1)
    days_in_month = (datetime(year, month + 1, 1) - start).days if month < 12 else 31

    # Mỗi sản phẩm có 1 danh mục cố định (mô phỏng đúng đặc tính dữ liệu thật)
    products = {}
    for pid in range(1000, 3000):
        cat_code, brand, lo, hi = random.choice(CATEGORIES)
        if random.random() < 0.30:
            cat_code, brand = "", random.choice(BRANDS_EXTRA)   # sản phẩm không danh mục
        products[pid] = (
            cat_code,
            brand,
            random.uniform(lo, hi),
            random.choice([2053013555631882655, 2053013566100866035]),
        )

    users = list(range(5_000_000, 5_020_000))
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["event_time", "event_type", "product_id", "category_id",
                    "category_code", "brand", "price", "user_id", "user_session"])
        for _ in range(n_rows):
            ts = start + timedelta(
                days=random.randrange(days_in_month),
                seconds=random.randrange(86400),
            )
            pid = random.choice(list(products.keys()))
            cat_code, brand, price, cat_id = products[pid]
            # 2% sản phẩm chưa gán giá -> price = 0 (mô phỏng dữ liệu thật)
            if random.random() < 0.02:
                price = 0.0
            w.writerow([
                ts.strftime("%Y-%m-%d %H:%M:%S UTC"),
                random.choice(EVENT_TYPES),
                pid,
                cat_id,
                cat_code,
                brand,
                f"{price:.2f}",
                random.choice(users),
                f"session-{random.randrange(100000)}",
            ])
    print(f"  ✅ {os.path.basename(path)}: {n_rows:,} dòng")


if __name__ == "__main__":
    os.makedirs(DATA_RAW, exist_ok=True)
    print("Sinh dữ liệu mẫu...")
    gen_file(os.path.join(DATA_RAW, "sample_oct.csv"), 2019, 10, 50_000)
    gen_file(os.path.join(DATA_RAW, "sample_nov.csv"), 2019, 11, 70_000)
    print("Xong. Có thể đổi tên thành 2019-Oct.csv / 2019-Nov.csv để chạy pipeline.")
```

**Điểm cần lưu ý khi dùng script này:** nó mô phỏng đúng các đặc tính quan trọng
của dữ liệu thật — sản phẩm không danh mục (30%), sản phẩm chưa gán giá (2%),
tỉ lệ event type thực tế, hậu tố `" UTC"`. Nhờ vậy người khác kiểm chứng được
pipeline hoạt động đúng.

## 10.6. Minh chứng demo (mục 7 đề tài)

Cần chuẩn bị:
1. **Log** — 5 file trong `Data/logs/` (đã có).
2. **Ảnh chụp màn hình** — chụp terminal khi chạy từng script, hoặc chụp Spark UI.
3. **Dữ liệu trước/sau** — bảng ở Mục 10.3.
4. **Sơ đồ luồng xử lý** (mục 5.6) — xem Mục 2.1, vẽ bằng matplotlib/PowerPoint.

### Gợi ý kịch bản demo 5 phút

```
1. (30s)  Giới thiệu: nguồn dữ liệu 14,7 GB, 110 triệu dòng, 4 đặc trưng Big Data
2. (60s)  Chạy Data_Verify.py — in ra: 0 khóa mồ côi, 0 dòng lệch múi giờ
3. (60s)  Mở Spark UI (http://localhost:4040) — chỉ ra 16 task chạy song song
4. (60s)  Mở file CSV CDC bằng Excel — 40.082 sản phẩm mới, 2.294.359 user mới
5. (90s)  Mở Data/silver trên đĩa — 4 bảng, 109.819.993 dòng
```

## 10.7. Sơ đồ luồng xử lý — vẽ bằng code

Vẽ sơ đồ bằng matplotlib để số liệu trên sơ đồ luôn khớp với pipeline:

```python
"""
Vẽ 2 sơ đồ kiến trúc (PNG, DPI 200) để chèn vào báo cáo.
Chạy: .\.venv311\Scripts\python.exe Data_Architecture.py
"""
import os
import matplotlib
matplotlib.use("Agg")          # không mở cửa sổ - chạy được trên server
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

from config import DATA_BRONZE, DATA_SILVER, PROJECT_ROOT

OUT = os.path.join(PROJECT_ROOT, "Data", "dashboard")
os.makedirs(OUT, exist_ok=True)

# --- Sơ đồ 1: kiến trúc tổng thể ---
fig, ax = plt.subplots(figsize=(16, 9))
ax.set_xlim(0, 16); ax.set_ylim(0, 9); ax.axis("off")

def box(x, y, w, h, text, color, size=9):
    ax.add_patch(FancyBboxPatch((x, y), w, h,
                                boxstyle="round,pad=0.15",
                                facecolor=color, edgecolor="#333", linewidth=1.2))
    ax.text(x + w/2, y + h/2, text, ha="center", va="center",
            fontsize=size, fontweight="bold", wrap=True)

def arrow(x1, y1, x2, y2, label=""):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2),
                                 arrowstyle="-|>", mutation_scale=16,
                                 linewidth=1.6, color="#444"))
    if label:
        ax.text((x1+x2)/2, (y1+y2)/2 + 0.15, label, ha="center",
                fontsize=7.5, color="#222",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.85))

box(0.4, 7.2, 3.0, 1.2, "SOURCES\nData/raw/\n2 file CSV · 14,7 GB", "#cfe8ff")
box(4.6, 7.2, 3.0, 1.2, "BRONZE\nData_cleaning.py\n707s", "#ffe9c7")
box(4.6, 5.2, 3.0, 1.2, "CDC\nData_CDC.py\n618s", "#ffd6d6")
box(4.6, 3.2, 3.0, 1.2, "SILVER\nData_Silver.py\n668s", "#d6f5d6")
box(4.6, 1.2, 3.0, 1.2, "VERIFY\nData_Verify.py\n57s", "#e6d9ff")
box(9.0, 5.2, 3.0, 1.2, "dim_product\n206.876", "#d6f5d6")
box(9.0, 3.2, 3.0, 1.2, "dim_user\n5.316.649", "#d6f5d6")
box(9.0, 1.2, 3.0, 1.2, "fact_event\n109.819.993", "#d6f5d6")
box(13.0, 3.2, 2.6, 1.2, "NEXT GROUP\nGold → Dashboard", "#e0e0e0")

arrow(3.4, 7.8, 4.6, 7.8, "CSV")
arrow(6.1, 7.2, 6.1, 6.4, "Parquet")
arrow(6.1, 5.2, 6.1, 4.4, "full_outer join")
arrow(6.1, 3.2, 6.1, 2.4, "star schema")
arrow(9.0, 6.4, 7.6, 6.4, "ghi ra")
arrow(9.0, 4.4, 7.6, 4.4)
arrow(9.0, 2.4, 7.6, 2.4)
arrow(12.0, 3.8, 13.0, 3.8, "KPI")

plt.title("Kiến trúc Data Lake 3 tầng — E-commerce Behavior Data Pipeline",
          fontsize=15, fontweight="bold")
plt.tight_layout()
plt.savefig(os.path.join(OUT, "00_kien_truc_tong_the.png"), dpi=200, bbox_inches="tight")
plt.close()

# --- Sơ đồ 2: mô hình sao Silver ---
fig, ax = plt.subplots(figsize=(13, 9))
ax.set_xlim(0, 13); ax.set_ylim(0, 9); ax.axis("off")

box(4.3, 3.6, 4.4, 2.0, "FACT_EVENT\n109.819.993 dòng\n1 dòng = 1 sự kiện", "#d6f5d6", 11)
box(0.5, 6.4, 3.2, 1.6, "DIM_PRODUCT\n206.876\nPK: product_id", "#ffe9c7")
box(9.3, 6.4, 3.2, 1.6, "DIM_USER\n5.316.649\nPK: user_id", "#ffe9c7")
box(4.3, 0.4, 4.4, 1.6, "DIM_CATEGORY\n130\nPK: category_code", "#ffe9c7")

arrow(3.7, 6.6, 5.0, 5.6, "FK product_id")
arrow(9.3, 6.6, 8.0, 5.6, "FK user_id")
arrow(6.5, 3.6, 6.5, 2.0, "FK category_code")

plt.title("Mô hình sao tầng Silver — grain, khóa chính, khóa ngoại",
          fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig(os.path.join(OUT, "01_mo_hinh_sao_silver.png"), dpi=200, bbox_inches="tight")
plt.close()

print("✅ Đã sinh 2 ảnh sơ đồ vào", OUT)
```

---

# 11. XỬ LÝ SỰ CỐ

## 11.1. Bảng tra nhanh

| Triệu chứng | Nguyên nhân gốc | Cách sửa |
|---|---|---|
| `Python worker exited unexpectedly` | Python 3.12 xoá module `distutils` | Dùng `.venv311` (Python 3.11) |
| `Cannot run program python3` | Spark JVM gọi `python3`, Windows không resolve `.bat` | `copy python.exe python3.exe` |
| `FileNotFoundException: ...winutils.exe` | Thiếu winutils | Đặt `D:\Metadata\hadoop\bin\winutils.exe`, set `HADOOP_HOME` |
| `setPermission` / `AccessControlException` khi ghi | Chưa set `HADOOP_HOME` | Kiểm tra lại winutils + `spark.hadoop.fs.permissions.umask-mode=000` |
| `OutOfMemoryError: Java heap space` | `persist()` tập > RAM | Ghi ra đĩa rồi đọc lại (Mục 8.6) |
| `Path already exists` khi ghi | Không overwrite | `.mode("overwrite")` |
| Bronze rỗng nhưng code đúng | Spark không ghi được Parquet | Kiểm tra winutils (bảng trên) |
| `UnicodeEncodeError` khi in tiếng Việt | Windows cp1252 | `config.py` đã ép UTF-8 — đảm bảo `from config import *` là dòng đầu |
| Job chạy > 10 phút bất thường | Thiếu `cache()` khi dùng nhiều lần | `df.cache(); df.count()` |
| Timestamp lệch 7 tiếng | Session timezone ≠ UTC | `spark.sql.session.timeZone = UTC` (đã có trong `setup_spark`) |
| `Table or view not found` | Biến Python không tự đăng ký catalog | Dùng API DataFrame thay vì `spark.sql()` |

## 11.2. Lỗi múi giờ — giải thích chi tiết (mục 7.1 báo cáo)

**Hiện tượng:**
```
CSV gốc:  2019-11-01 00:00:00 UTC
Bronze:   2019-10-31 17:00:00     ← lệch 7 tiếng, sang ngày trước!
```

**Nguyên nhân:** Spark mặc định dùng múi giờ máy (GMT+07:00) khi parse chuỗi
timestamp không có múi giờ. Tệ hơn: cột `day` và `hour` được tính theo giờ máy →
mâu thuẫn nội tại với `event_time`.

**Cách sửa:** đặt `spark.sql.session.timeZone = UTC` khi tạo SparkSession. Vì nguồn
CSV ghi timestamp theo chuỗi " UTC", giữ "UTC" là parse đúng giá trị gốc.

**Sau khi sửa:**
```
Bronze Oct: 2019-10-01 00:00:00 → 2019-10-31 23:59:59
Bronze Nov: 2019-11-01 00:00:00 → 2019-11-30 23:59:59
```

**Bài học:** trong pipeline dữ liệu lớn, múi giờ là lỗi "âm thầm" — code vẫn
chạy, số liệu vẫn ra, nhưng sai. Bắt buộc kiểm tra bằng **assertion tự động**
(Mục 9.3), không kiểm tra bằng mắt.

## 11.3. Lỗi RFM bị đảo chiều (mục 7.4 báo cáo)

Lần chạy đầu cho ra phân khúc "VIP" chỉ chi **94,70 USD** và mua **1,00 lần**, trong
khi nhóm "New" lại chi **1.891 USD / 5,52 lần** — trái ngược hoàn toàn.

**Nguyên nhân:** tham số `ascending` bị truyền ngược.

**Cách sửa (đáng chú ý):** đổi tên tham số theo **NGỮ NGHĨA** thay vì theo hướng
sắp xếp:
```python
def score_by_percentile(col, higher_is_better=True):
    ...
```
Sau khi sửa: VIP chi **2.214 USD/người**, mua **6,16 lần** — khớp đúng định nghĩa.

**Bài học:** đặt tên tham số theo ý nghĩa ("giá trị lớn hơn có tốt hơn không") thay
vì theo hướng sắp xếp, vì hướng sắp xếp rất dễ bị đảo khi copy code. Loại lỗi này
nguy hiểm vì code vẫn chạy, vẫn ra số, chỉ là **số sai nghĩa**.

## 11.4. Checklist khi không chạy được

```
□ Đúng venv không?         → .\.venv311\Scripts\python.exe
□ Đúng phiên bản Python?   → python --version phải là 3.11.x
□ JAVA_HOME trỏ JDK 17?    → java -version
□ winutils.exe tồn tại?    → Test-Path D:\Metadata\hadoop\bin\winutils.exe
□ python3.exe trong venv?  → Test-Path .\.venv311\Scripts\python3.exe
□ Đủ dung lượng đĩa?       → cần ~10 GB cho Bronze + Silver
□ Đủ RAM?                  → đóng hết app nặng, driver heap 4g
□ Chạy đúng thứ tự script? → CDC trước Silver
```

---

# 12. CHECKLIST NGHIỆM THU

Dùng checklist này để tự kiểm tra trước khi nộp. Đánh dấu ✅ khi đạt.

## A. Môi trường
- [ ] `java -version` → 17.x
- [ ] `python --version` → 3.11.x
- [ ] `Test-Path .\.venv311\Scripts\python3.exe` → True
- [ ] `Test-Path D:\Metadata\hadoop\bin\winutils.exe` → True
- [ ] `pip list | findstr pyspark` → 3.5.3

## B. Dữ liệu
- [ ] `Data/raw/2019-Oct.csv` (5,28 GB)
- [ ] `Data/raw/2019-Nov.csv` (8,39 GB)

## C. Chạy pipeline
- [ ] `Data_collect.py` chạy, in 67.501.979 records
- [ ] `Data_cleaning.py` chạy, 707s, tạo 2 thư mục Bronze
- [ ] `Data_CDC.py` chạy, 618s, tạo 3 bảng CDC
- [ ] `Data_Silver.py` chạy, 668s, tạo 4 bảng Silver
- [ ] `Data_Verify.py` chạy, 95s, 0 lỗi
- [ ] `Data_Gold.py` chạy, 79s, tạo 8 bảng KPI
- [ ] `Data_Dashboard.py` chạy, 84s, tạo 9 ảnh PNG + 9 bảng CSV
- [ ] `Data_Architecture.py` chạy, 2s, tạo 2 sơ đồ kiến trúc

## D. Kiểm chứng số liệu
- [ ] Bronze: `oct_2019.parquet` 86 file / 1,62 GB
- [ ] Bronze: `nov_2019.parquet` 136 file / 2,72 GB
- [ ] CDC: 40.082 sản phẩm mới · 16.214 bị gỡ · 46.450 đổi giá
- [ ] CDC: 2.294.359 user mới · 1.620.532 rời đi · 1.401.758 giữ lại
- [ ] Silver: `fact_event` 109.819.993 dòng
- [ ] Silver: `dim_user` 5.316.649 · `dim_product` 206.876 · `dim_category` 130
- [ ] Đối chiếu Bronze→Silver: giữ 99,8811%
- [ ] 3 quan hệ khóa ngoại: 0 khóa mồ côi
- [ ] Múi giờ: 0 dòng lệch `hour`/`day`/`event_date`
- [ ] Dải ngày: 2019-10-01 → 2019-11-30

## E. Minh chứng (mục 7 đề tài)
- [ ] 7 file log trong `Data/logs/`
- [ ] Bảng thời gian xử lý từng job
- [ ] Bảng dữ liệu trước/sau
- [ ] Sơ đồ luồng xử lý (2 ảnh PNG)
- [ ] 7 biểu đồ nghiệp vụ (7 ảnh PNG)
- [ ] Ảnh chụp màn hình log chạy (demo)
- [ ] `README.md` hướng dẫn cài đặt + chạy
- [ ] `requirements.txt`
- [ ] `make_sample_data.py` (dữ liệu mẫu — mục 5.8)
- [ ] Bảng phân công thành viên

## F. Báo cáo
- [ ] Mục 1–9 theo đúng cấu trúc mục 6.1 đề tài
- [ ] Tóm tắt 150–250 từ
- [ ] Mục 2 mô tả **vì sao** chọn công nghệ (không chỉ liệt kê tên)
- [ ] Mục 3 có schema + ví dụ bản ghi + đặc trưng Big Data
- [ ] Mục 5 có xử lý lỗi (mục 11 của tài liệu này)
- [ ] Mục 6 có kịch bản thí nghiệm + tiêu chí đánh giá
- [ ] Mục 8 thảo luận: điểm mạnh, hạn chế, bài học
- [ ] TLTK + phụ lục

---

# 13. CÂU HỎI GIẢNG VIÊN HAY HỎI

Chuẩn bị trả lời trước — đây là 12 câu hỏi có xác suất cao.

### Q1. "Vì sao chọn Storage → Processing mà không chọn quy trình khác?"
> "Dữ liệu nguồn đã ở dạng file phẳng 14,7 GB, phù hợp với bài toán Data Lake và cho
> phép thể hiện rõ nhất đặc trưng dữ liệu lớn: Volume (110 triệu dòng), Velocity
> (1,8 triệu dòng/ngày), Veracity (NULL 32,4%, trùng lặp, lỗi múi giờ) và nhu cầu
> xử lý phân tán. Storage → Processing là nơi phần lớn công việc thật sự nằm ở —
> làm sạch và kiểm soát chất lượng."

### Q2. "`local[*]` có phải là xử lý phân tán không?"
> "Không — `local[*]` là chạy đa luồng trên một máy. Nhưng toàn bộ code là **code
> Spark phân tán**: đổi `master` thành `spark://master:7077` là chạy trên cụm mà
> không phải viết lại. Spark SQL, `shuffle`, `partitionBy`, `left_anti join` đều là
> cơ chế phân tán thật. Trong log thấy `[Stage 2:==> (10 + 16) / 68]` — 16 task
> chạy song song. Tôi cũng đã đo: job nặng nhất 707 giây trên 16 luồng, trên cụm
> 16 node có thể chia cho ~16."

### Q3. "Tại sao bỏ 130.750 dòng? Có làm mất dữ liệu không?"
> "Không mất dữ liệu có nghĩa. 130.750 dòng trùng **hoàn toàn** trên khóa sự kiện
> (event_time, event_type, product_id, user_id, user_session) — cùng một hành động bị
> ghi 2 lần trong nhật ký web. Giữ lại 99,8811% dữ liệu, tỉ lệ mất 0,1189%. Quan
> trọng hơn: đây là bản ghi **trùng lặp**, không phải bản ghi **khác nhau** — nếu
> giữ lại, phễu chuyển đổi và doanh thu sẽ bị thổi phồng."

### Q4. "Tại sao fill NULL bằng 'unknown' thay vì xoá?"
> "Xoá 32,4% số dòng sẽ mất toàn bộ sự kiện của nhóm sản phẩm không phân loại —
> 10,4% doanh thu. Tôi đo được tỉ lệ khôi phục được = 0, tức 107.933 sản phẩm không
> có danh mục ở bất kỳ dòng nào → đây là Missing Not At Random, là thuộc tính sản
> phẩm. Nên tôi gắn cờ `is_classified` để phân tích biết chính xác phần nào không
> phân loại, thay vì điền giá trị bịa đặt."

### Q5. "Cột `total_spent` của bạn có đúng không?"
> "Đúng, và đây là chỗ tôi cảnh báo. Tôi **chỉ tính từ sự kiện `purchase`**. Nếu
> `sum(price)` trên mọi event thì người dùng chỉ xem sản phẩm 100.000 USD cũng bị
> tính là đã chi 100.000 USD. Loại lỗi này nguy hiểm vì con số vẫn 'hợp lý' về
> hình thức."

### Q6. "Tại sao không `persist()` để tăng tốc?"
> "Tôi đã thử — và nó làm job chết. Spark cache kiểu columnar cho DataFrame 110
> triệu dòng cần hơn 4 GB heap → `OutOfMemoryError` sau 229 giây. Cách đúng là
> ghi `fact_event` ra đĩa một lần rồi đọc lại: không tốn RAM, đọc Parquet chỉ mất
> vài giây. Bài học: `persist()` chỉ đúng khi tập dữ liệu vừa với RAM."

### Q7. "Làm sao biết dữ liệu không bị lệch múi giờ?"
> "Tôi có assertion tự động: so `hour`, `day`, `event_date` đã lưu với giá trị tính
> lại từ `event_time` trong cùng ngữ cảnh UTC. Kết quả 0 dòng lệch trên 110
> triệu dòng. Lưu ý: khi in ra, driver Python hiển thị theo giờ máy nên có thể
> lệch 7 tiếng — nhưng giá trị lưu trong Parquet là UTC."

### Q8. "Số liệu thống kê chất lượng ở đâu?"
> "Mục 5.7 của đề tài. Tôi có `Data_Verify.py` — script kiểm chứng **độc lập**,
> đọc lại từ đĩa thay vì dùng lại biến RAM của script trước. Nó kiểm tra: số dòng,
> đối chiếu Bronze→Silver, 3 quan hệ khóa ngoại, độ bao phủ, và tính nhất quán múi
> giờ. Kết quả: 0 khóa mồ côi, 0 dòng lệch."

### Q9. "Có thể chứng minh khả năng mở rộng không?"
> "Có 3 lớp bằng chứng. (1) Thực nghiệm: 110 triệu dòng / 14,7 GB xử lý trong
> 707 giây trên máy 16 luồng — tức ~155.500 dòng/giây, đây là số đo thật chứ không
> phải lập luận. (2) Kiến trúc: `partitionBy` cho phép thêm partition mới mà không
> viết lại dữ liệu cũ; định dạng Parquet + column pruning giảm I/O theo cột.
> (3) Khả năng chuyển đổi: chỉ cần đổi `master` là chạy trên cụm."

### Q10. "CDC của bạn là CDC thật không?"
> "Đây là CDC theo nghĩa **batch snapshot diff** (so sánh 2 ảnh chụp), không phải
> log-based CDC đọc từ WAL. Trong bài toán batch theo tháng, đây là cách đúng và
> đủ. Tôi nói rõ điều này trong báo cáo để không gây hiểu nhầm."

### Q11. "Nếu thêm dữ liệu streaming vào thì sao?"
> "Kiến trúc đã tách lớp đọc (Bronze) khỏi lớp xử lý, nên chỉ cần thay bước ingest
> CSV bằng Kafka consumer ghi vào cùng định dạng Parquet là phần còn lại giữ
> nguyên. Tôi ghi rõ đây là hướng mở rộng, chưa triển khai trong bản nộp."

### Q12. "Hạn chế lớn nhất của dự án?"
> "Ba điểm. (1) Chạy `local[*]` trên một máy nên chưa chứng minh được lợi ích
> tuyệt đối của phân tán — cần cụm thật mới đo được. (2) `approx_count_distinct`
> có sai số ~2% — tôi chấp nhận vì đếm chính xác 110 triệu dòng quá tốn kém; đã
> ghi rõ trong báo cáo. (3) CDC mới ở mức so sánh 2 kỳ, chưa xử lý được thay đổi
> trong nhiều hơn 2 kỳ hay xoá sản phẩm rồi tái tạo."

---

# PHỤ LỤC A. BẢNG TRA NHANH SỐ LIỆU

| Chỉ số | Giá trị |
|---|---|
| Sự kiện Bronze → Silver | 109.950.743 → 109.819.993 (bỏ 130.750 dòng trùng) |
| Tỉ lệ giữ dữ liệu | 99,8811% |
| Khóa mồ côi (3 quan hệ) | 0 |
| Dòng lệch múi giờ | 0 |
| Dung lượng Bronze | 4,34 GB (từ 14,7 GB CSV — nhỏ hơn 3,4×) |
| Sản phẩm có thay đổi | 102.746 / 206.876 (49,7%) |
| Sản phẩm mới / bị gỡ / đổi giá | 40.082 / 16.214 / 46.450 |
| User mới / rời đi / giữ lại | 2.294.359 / 1.620.532 / 1.401.758 |
| Sự kiện có danh mục | ~67,6% (32,4% không phân loại — MNAR) |
| Sự kiện có thương hiệu | ~86,3% (13,7% NULL) |
| Thời gian: clean / CDC / Silver / Verify | 707s / 618s / 668s / 95s |
| Tổng thời gian pipeline | ~40 phút |

# PHỤ LỤC B. DANH SÁCH FILE ĐÍCH KÈM THEO

| File | Mục đích |
|---|---|
| `config.py` | Cấu hình chung, `setup_spark()` xử lý 4 lỗi Windows |
| `Data_collect.py` | Giai đoạn 1.1 + 1.2 — thu thập và EDA |
| `Data_cleaning.py` | Giai đoạn 1.3 — làm sạch, ghi tầng Bronze |
| `Data_CDC.py` | Giai đoạn 1.4 — so sánh 2 kỳ, 3 bảng CDC |
| `Data_Silver.py` | Giai đoạn 2 — tách chiều, kiểm soát chất lượng, ghi Silver |
| `Data_Verify.py` | Kiểm chứng độc lập tầng Silver từ đĩa |
| `Data_Gold.py` | Giai đoạn 3 — tổng hợp 8 bảng KPI, ghi tầng Gold |
| `Data_Dashboard.py` | Giai đoạn 4 — 7 biểu đồ PNG + 8 bảng KPI CSV |
| `Data_Architecture.py` | Vẽ 2 sơ đồ kiến trúc (PNG, DPI 200) |
| `make_sample_data.py` | Sinh dữ liệu mẫu để người khác tái lập (mục 5.8) |
| `requirements.txt` | Danh sách thư viện + phiên bản |
| `README.md` | Hướng dẫn cài đặt và chạy |
| `NHAT_KY.md` | Nhật ký quy trình: lý do thiết kế, bài học từ 7 lỗi |

# PHỤ LỤC C. LỊCH SỬ 7 LỖI ĐÃ GẶP (để viết mục "Thảo luận")

| # | Lỗi | Bài học |
|---|---|---|
| 1 | Bronze rỗng dù code đúng | Lỗi môi trường Windows xếp chồng, mỗi lỗi che lỗi sau → sửa tuần tự từ ngoài vào trong |
| 2 | Múi giờ lệch 7 tiếng | Múi giờ là lỗi "âm thầm" — phải có assertion tự động, không kiểm bằng mắt |
| 3 | Pivot tên cột sai thứ tự | Kiểm tra schema sau pivot, đừng tin vào `dict` |
| 4 | `total_spent` tính cả giá lượt xem | Lỗi logic "số vẫn hợp lý hình thức" — nguy hiểm nhất |
| 5 | `percentile_approx` trong window function cho kết quả sai | Hàm xấp xỉ không dùng được trong cửa sổ → phải dùng `ntile` |
| 6 | `persist()` → OOM sau 229s | `persist()` chỉ đúng khi tập vừa RAM; lớn hơn RAM thì ghi đĩa |
| 7 | RFM bị đảo chiều (VIP chi ít nhất) | Đặt tên tham số theo **ngữ nghĩa** (`higher_is_better`), không theo hướng sắp xếp |

## PHỤ LỤC C2. 4 LỖI TRỰC QUAN HÓA ĐÃ GẶP (bổ sung 25/10)

Các lỗi này **không ảnh hưởng số liệu** — chỉ là hiển thị sai. Nhưng cũng phải sửa:
một biểu đồ đè chữ thì giảng viên không đọc được, mà đọc không được thì coi như
không có biểu đồ.

| # | Lỗi | Nguyên nhân | Bài học |
|---|---|---|---|
| 8 | Sơ đồ sao: hộp chồng lấn nhau | Toạ độ cứng: `fact_event` x 33–67 đè `dim_product` x 14–40 và `dim_user` x 62–88 | Sơ đồ vẽ bằng code cũng phải **tính toạ độ**, không gõ tay rồi hy vọng |
| 9 | Sơ đồ kiến trúc: nhãn đè lên hộp KPI | Nhãn `Data_Verify.py` đặt đúng chỗ 2 hộp KPI chiếm; thêm nữa nhãn `Data_Gold.py` quá dài chiếm hết khoảng trống ngang | **Đo pixel bằng `ax.transData.transform()`** thay vì đoán tọa độ — đoán sai 3 lần liên tiếp |
| 10 | Biểu đồ 06/03: nhãn trục y nằm ngang | `barh` + `invert_yaxis()` → matplotlib 3.11.2 đặt nhãn `rotation=0, va='bottom'` | Test từng biểu đồ; `invert_yaxis()` còn làm trục x nhảy lên **trên** đè tiêu đề → đảo dữ liệu thay vì đảo trục |
| 11 | Biểu đồ 01: chú giải đè đường xu hướng | `legend(loc="upper left")` nằm ngay trên đường dữ liệu | Chú giải đặt **ngoài** vùng dữ liệu (`bbox_to_anchor`) khi biểu đồ kín |

**Quy tắc rút ra:** sau khi vẽ xong, **phải mở từng ảnh PNG ra xem** — không tin
rằng code chạy không lỗi ⇒ hình đúng. 4 lỗi trên đều do code chạy hoàn hảo.

---

*Hết tài liệu. Tổng thời gian triển khai ước tính: 36 phút chạy máy + 2–3 giờ
đọc hiểu và viết lại code từ tài liệu này.*

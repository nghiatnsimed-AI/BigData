# NHẬT KÝ ĐỒ ÁN BIG DATA — E-commerce Behavior Data Pipeline

> Ghi lại **quy trình**, **lý do tại sao làm vậy**, và **kết quả mong muốn** của từng bước.
> Cập nhật liên tục sau mỗi hoàn thành.

---

## MỐC 1 — Giai đoạn 1: Thu thập & Làm sạch ✅ HOÀN THÀNH (24/10/2026)

### 1.1 Ingestion — `Data_collect.py`
**Quy trình:** Spark đọc CSV → đếm records → in schema → in sample.
**Vì sao:** xác nhận cấu trúc đầu vào trước khi làm bất cứ việc gì khác.
**Kết quả mong muốn:** biết chính xác 9 cột, số bản ghi, kiểu dữ liệu gốc.
**Kết quả thực tế:** Nov 67.501.979 dòng · Oct 42.448.764 dòng · tất cả cột kiểu string.

### 1.2 EDA — `Data_collect.py`
**Quy trình:** schema → 5 dòng mẫu → `describe()` → đếm NULL từng cột.
**Vì sao:** biết trước chỗ nào có dữ liệu bẩn để thiết kế quy tắc làm sạch.
**Kết quả mong muốn:** bản đồ vùng trống của dữ liệu.
**Kết quả thực tế:** `category_code` 32,4% NULL · `brand` 13,7% NULL · `user_session` 10 NULL.

### 1.3 Cleaning — `Data_cleaning.py` (707 giây)
**Quy trình:**
1. Bỏ hậu tố `" UTC"` → parse `yyyy-MM-dd HH:mm:ss`
2. Ép kiểu: `product_id`/`category_id`/`user_id` → `long`, `price` → `double`
3. Fill NULL: `category_code` → `unknown.unknown`, `brand`/`user_session` → `unknown`
4. Tạo 9 cột phái sinh: `year`, `month`, `day`, `hour`, `date`, `event_date`, `category_level1/2/3`
5. Ghi Parquet `partitionBy(year, month)`, nén Snappy

**Vì sao múi giờ phải set UTC:**
> Nguồn CSV ghi `"2019-11-01 00:00:00 UTC"`. Spark mặc định parse theo giờ máy (`GMT+07:00`)
> → lưu vào Parquet thành `2019-10-31 17:00:00`, **lệch 7 tiếng**.
> Tệ hơn: cột `day` và `hour` được tính theo giờ máy → **mâu thuẫn nội tại** với `event_time`.
> Lỗi này không làm script crash nên rất dễ bỏ qua, nhưng mọi phân tích theo giờ/ngày đều sai.
> Fix: `.config("spark.sql.session.timeZone", "UTC")` trong `setup_spark()`.

**Kết quả mong muốn:** Bronze có schema sạch, kiểu đúng, 18 cột, partition theo thời gian.
**Kết quả thực tế:** `oct_2019.parquet` 86 file 1,62 GB · `nov_2019.parquet` 136 file 2,72 GB.
**Kiểm chứng:** dải thời gian Bronze khớp chính xác kỳ dữ liệu (01/10→31/10, 01/11→30/11).

### 1.4 CDC — `Data_CDC.py` (618 giây)
**Quy trình:** `full_outer join` trên `product_id` giữa 2 kỳ → phân loại `change_type` →
so sánh tập `user_id` → phân loại `user_status` → tổng hợp theo `category_code`.
**Vì sao full_outer:** giữ được cả sản phẩm chỉ có ở 1 kỳ — đó chính là sản phẩm mới và sản phẩm bị gỡ.
**Vì sao `cache()`:** cùng một dữ liệu dùng cho nhiều phép aggregate; không cache thì mỗi phép lại
đọc toàn bộ Parquet từ đĩa (308s → 166s).
**Vì sao `approx_count_distinct`:** đếm user unique chính xác trên 110M rows tốn shuffle nặng;
sai số ~2% là chấp nhận được cho thống kê "số user".
**Kết quả mong muốn:** biết sản phẩm nào mới / bị gỡ / đổi giá, user nào mới / rời đi.
**Kết quả thực tế:** 206.876 sản phẩm · 40.082 mới · 16.214 bị gỡ · 46.450 đổi giá ·
2.294.359 user mới · 1.620.532 user rời đi · tăng trưởng event +59,0%.

**Phát hiện khi kiểm chứng:** nhiều sản phẩm có `price_oct = 0.0` kèm `price_delta` hàng nghìn.
Quét lại CSV gốc 9 GB xác nhận **giá 0 là dữ liệu thật** (68.673 dòng Oct, 188.088 dòng Nov,
0 dòng NaN). Nghĩa là sản phẩm chưa được gán giá trong tháng 10 và chỉ có giá từ tháng 11 —
đây là insight kinh doanh thật, không phải lỗi dữ liệu.

---

## MỐC 2 — Giai đoạn 2: Silver Layer ✅ HOÀN THÀNH (24/10/2026)

### Vì sao cần Silver?
> Bronze là **dữ liệu thô đã chuẩn hóa kiểu** — còn dạng "sự kiện" (event log), mỗi dòng là một hành động.
> Để phân tích, cần cấu trúc **tách chiều**: sản kiện, sản phẩm, người dùng, danh mục là 4 thực thể riêng biệt.
> Silver là nơi tách chiều + khôi phục chất lượng dữ liệu ở mức có thể tin cậy được.

### Thiết kế 4 bảng Silver

| Bảng | Grain (1 dòng = ?) | Nội dung |
|---|---|---|
| `dim_product` | 1 sản phẩm | product_id, category_code, brand, giá, danh mục 3 cấp |
| `dim_user` | 1 người dùng | user_id, số session, kỳ hoạt động, hành vi |
| `dim_category` | 1 danh mục | category_code, cấp 1-3, số sản phẩm, doanh thu |
| `fact_event` | 1 sự kiện | khóa ngoại tới 3 chiều + event_time + event_type + price |

### Quy trình 6 bước của Silver
1. **Khảo sát Bronze** — đo trùng lặp, đo tỉ lệ khôi phục được, đo kích thước chiều
2. **Phân loại danh mục** — đánh dấu sản phẩm có/không thuộc danh mục nào
3. **Tách chiều** — dựng 3 bảng dimension + 1 bảng fact
4. **Tích hợp CDC** — gắn nhãn `change_type` cho sản phẩm, `user_status` cho user
5. **Ghi Silver** — Parquet partitionBy, có kiểm tra referential integrity
6. **Báo cáo** — số dòng mỗi bảng, độ bao phủ, thời gian xử lý

---

### 🔎 KHẢO SÁT BƯỚC 1 — Kết quả và một sai lầm đã sửa

**Sai lầm ban đầu của tôi:** thiết kế Silver theo ý "khôi phục category_code/brand bị thiếu bằng cách
suy ra từ cùng `product_id`". Nghe rất hay, nhưng khi đo đạc thì **khôi phục được 0 dòng**.

**Điều tra gốc rễ (3 bước):**
1. Đo trên Bronze → 21.898.171 dòng thiếu category, khôi phục được 0.
   → Nghi do nguyên nhân: Bronze đã fill NULL thành `"unknown.unknown"`, mất dấu vết NULL gốc.
2. Quét lại **CSV gốc** (nơi NULL còn nguyên) → vẫn 0 dōng khôi phục được.
   → Nghi do nguyên nhân nằm ở bản chất dữ liệu, không phải ở pipeline.
3. Phân tích theo **mức độ đầy đủ của từng sản phẩm**:

| Nhóm sản phẩm | Số lượng | Tỉ lệ | Dòng null |
|---|---|---|---|
| Có category ở **mọi** dòng | 82.729 | 43,39% | 0 |
| **Không** có category ở bất kỳ dòng nào | 107.933 | 56,61% | 21.898.171 |
| Sản phẩm có category lẫn null (khôi phục được) | **0** | 0% | 0 |

**Kết luận:** NULL ở `category_code` là **Missing Not At Random (MNAR)** — không phải dữ liệu hỏng,
mà là **thuộc tính của sản phẩm**: 56,61% sản phẩm thực sự không thuộc danh mục nào.
Mỗi sản phẩm thuộc đúng một danh mục; sản phẩm không có danh mục thì **mọi dòng** đều null.

**Cách xử lý đúng (thay vì bịa dữ liệu):**
> Không fill bằng giá trị bịa đặt, mà **đánh dấu minh bạch** bằng cột cờ `is_classified` / `is_branded`.
> - `is_classified = True` → sản phẩm có danh mục, dùng cho phân tích danh mục
> - `is_classified = False` → sản phẩm không phân loại, vẫn giữ trong fact nhưng **loại khỏi mẫu số doanh thu theo danh mục**
> - Cột cờ cho phép trả lời: "doanh thu bị thiếu bao nhiêu do sản phẩm không phân loại?"
>
> **Bài học:** "khôi phục dữ liệu" là kỹ thuật, nhưng **trước khi khôi phục phải chứng minh được
> dữ liệu khôi phục là có thật**. Nếu không, kết quả là điền rác vào pipeline và mọi phân tích
> phía sau sẽ sai mà không ai biết. Trường hợp này khiến tôi chuyển từ "fill giá trị" sang
> "gắn nhãn + đo lường độ bao phủ", đây mới là cách xử lý dữ liệu MNAR đúng.

### Các số liệu khác đo được ở bước khảo sát
| Đại lượng | Giá trị |
|---|---|
| Bản ghi trùng hoàn toàn (Nov) | 100.530 dòng (0,1489%) |
| Sản phẩm có category/brand nhất quán | 99,93% |
| `price = 0` | 188.088 dòng (0,279%) — dữ liệu thật, sản phẩm chưa được gán giá |
| Số danh mục | 130 |
| Số thương hiệu | 4.202 |
| Số user (Nov) | 3.696.117 |
| Số session (Nov) | 13.776.051 |
| Số sản phẩm (Nov) | 190.662 |

### Kết quả mong muốn
- `dim_product` có ~190K dòng, cờ `is_classified` cho biết độ bao phủ danh mục
- `dim_user` có ~3,7M dòng, phân loại buyer / cart_only / viewer
- `fact_event` giữ ~110M dòng, mọi khóa ngoại hợp lệ
- Mọi số đo truy ngược được về dòng gốc

---

## ✅ KẾT QUẢ THỰC TẾ TẦNG SILVER (đã xác minh)

| Bảng | Số dòng | Dung lượng | File |
|---|---|---|---|
| `fact_event` | 109.819.993 | 3,91 GB | 32 |
| `dim_product` | 206.876 | 7,59 MB | 16 |
| `dim_user` | 5.316.649 | 164,91 MB | 16 |
| `dim_category` | 130 | 9,67 KB | 2 |

**Đối chiếu Bronze → Silver:** Bronze 109.950.743 → Silver 109.819.993.
Bỏ 130.750 bản ghi trùng lặp hoàn toàn, giữ lại 99,8811%.

**Referential integrity:** 0 khóa mồ côi trên cả 3 quan hệ → mô hình sao toàn vẹn.

**Độ bao phủ dữ liệu (sau khử trùng lặp):**
| Chỉ số | Có | Không có |
|---|---|---|
| Danh mục | 74.436.308 (67,78%) | 35.383.685 (32,22%) |
| Thương hiệu | 94.935.108 (86,45%) | 14.884.885 (13,55%) |
| Giá > 0 | 109.563.336 (99,77%) | 256.657 (0,23%) |

**Phân bố nhãn:**
- `customer_type`: viewer 4.130.958 · buyer 697.470 · cart_only 488.221
- `change_type`: UNCHANGED 104.130 · PRICE_CHANGED 46.450 · NEW_PRODUCT 40.082 · DISCONTINUED 16.214
- `user_status`: NEW_USER 2.294.359 · CHURNED_USER 1.620.532 · RETAINED 1.401.758

---

### 🐛 LỖI 1 — Bài học về hiệu năng: thiếu `persist()` làm pipeline chạy 6 lần

**Triệu chứng:** lần chạy Silver đầu tiên mất **35 phút** dù mọi phép tính riêng lẻ đều nhanh.

**Nguyên nhân:** biến `events` được dùng lại ở nhiều nơi (tạo 3 dimension, 1 fact,
3 phép kiểm tra khóa ngoại). Trong Spark, mỗi `.write()` và `.count()` trên một
DataFrame không được cache sẽ **tính lại toàn bộ chuỗi biến đổi từ đầu** —
tức là 6 lần đọc và xử lý lại 110 triệu dòng.

**Cách sửa:** `events.persist(StorageLevel.MEMORY_AND_DISK)` ngay sau bước khử trùng lặp,
rồi `events.count()` để materialize. Dùng `MEMORY_AND_DISK` chứ không phải `cache()`
mặc định (chỉ RAM) vì 110 triệu dòng không vừa RAM — nhưng spill xuống đĩa vẫn
nhanh hơn nhiều so với tính lại từ đầu.

**Cách sửa thứ hai (quan trọng hơn):** phần kiểm tra chất lượng giờ **đọc lại từ đĩa**
thay vì dùng biến trong bộ nhớ. Kiểm tra đúng thứ đã lưu xuống đĩa, và chỉ mất
vài giây thay vì hàng chục phút.

**Bài học:** trong Spark, `persist()` không phải tối ưu hoá — mà là **điều kiện cần**
khi một DataFrame được dùng nhiều lần. Và khi kiểm tra chất lượng, hãy kiểm tra
trên **dữ liệu đã ghi ra đĩa**, không phải trên biến trong bộ nhớ.

---

### 🐛 LỖI 2 — Hiển thị múi giờ gây hiểu nhầm

**Triệu chứng:** khi đọc Silver, `min(event_time)` hiển thị là `2019-10-01 07:00:00`
thay vì `2019-10-01 00:00:00` như trong CSV gốc.

**Điều tra:** kiểm tra chéo 3 cách cho kết quả:
| Cách kiểm tra | Kết quả |
|---|---|
| `hour(event_time)` vs cột `hour` | 0 dòng lệch |
| `dayofmonth(event_time)` vs cột `day` | 0 dòng lệch |
| `to_date(event_time)` vs cột `event_date` | 0 dòng lệch |
| Khoảng ngày: `event_date` vs tính lại từ `event_time` | 2019-10-01 → 2019-11-30, khớp |

**Kết luận:** dữ liệu **đúng**. Parquet lưu timestamp dưới dạng micro-giây kể từ
epoch UTC. Khi đọc lên, PySpark hiển thị theo múi giờ của Python driver (GMT+07:00),
nên giá trị bị dịch 7 tiếng **chỉ khi in ra**, không phải khi lưu.

**Bài học:** không được kết luận "lỗi múi giờ" chỉ từ chuỗi hiển thị. Phải kiểm tra
**tính nhất quán nội tại** — so sánh cột phái sinh với giá trị tính lại từ giá trị gốc
trong cùng ngữ cảnh. Cách kiểm tra này vừa nhanh vừa không phụ thuộc múi giờ máy.

**Cải tiến trong `Data_Verify.py`:** dùng `left_anti join` thay vì `left join` + filter
để kiểm tra khóa ngoại — không kéo theo payload, nhanh hơn nhiều.

---

### 🐛 LỖI 3 — Pivot tên cột sai thứ tự (lỗi dễ gặp)

**Triệu chứng:** `AnalysisException: A column or function parameter with name 'revenue_10' cannot be resolved.
Did you mean one of the following? ['10_revenue', '11_revenue', ...]`

**Nguyên nhân:** sau `pivot("month", [10, 11])`, Spark đặt tên cột theo thứ tự
**`<giá trị pivot>_<tên chỉ số>`** → `10_revenue`, **không phải** `<tên chỉ số>_<giá trị>`
→ `revenue_10`. Sai chỗ này rất dễ gây nhầm lẫn vì trực giác thường ngược lại.

**Cách sửa:** dùng đúng tên `10_revenue` / `11_purchases` khi tham chiếu sau pivot.

---

### 🐛 LỖI 4 — `total_spent` tính cả giá của lượt xem (lỗi logic nguy hiểm)

**Triệu chứng:** số "chi tiêu trung bình" mỗi người dùng là **17.631 USD**.

**Nguyên nhân:** `F.sum(F.col("price"))` trên **mọi** sự kiện. Nhưng `price` là giá niêm
yết của sản phẩm, mà **mọi** sự kiện đều mang giá đó — kể cả lúc người dùng chỉ
*xem* sản phẩm. Người dùng chỉ xem sản phẩm giá 100.000 USD cũng bị tính là
đã chi tiêu 100.000 USD.

**Cách sửa:** chỉ tính doanh thu từ sự kiện `purchase`:
```python
F.sum(F.when(F.col("event_type") == "purchase", F.col("price")).otherwise(0))
```

**Bài học:** đây là loại lỗi nguy hiểm nhất vì **con số vẫn trông hợp lý**.
17.631 USD/người không ai thấy vô lý, nhưng nó sai hoàn toàn.
Nguyên tắc: mọi phép cộng trên tiền phải có điều kiện lọc rõ ràng — không bao giờ
cộng "giá" của một sự kiện mà không phải giao dịch.

---

### 🐛 LỖI 5 — `percentile_approx` trong window function cho kết quả sai

**Triệu chứng:** phân khúc RFM chỉ có **một** nhóm duy nhất "VIP" với 697.470 người
(bằng đúng số buyer) — 5 phân khúc khác biến mất.

**Nguyên nhân:** `F.percentile_approx(col, 0.2).over(Window.partitionBy())` trả về
**một giá trị duy nhất** cho toàn bộ tập, nên mọi dòng đều so sánh với cùng một
ngưỡng và rơi vào cùng một nhóm. `percentile_approx` cần một tập dữ liệu hoàn chỉnh
để tính, không hoạt động như một hàm cửa sổ theo dòng.

**Cách sửa:** tính ngưỡng phân vị **một lần** ở dạng scalar:
```python
thresholds = rfm.agg(*[F.percentile_approx(c, q).alias(f"{c}_{q}") ...]).collect()[0]
```
rồi dùng `CASE WHEN` để chấm điểm 1-5 dựa trên các ngưỡng đó.

**Bài học:** khi một hàm tổng hợp trả về **một giá trị cho cả tập** (như
`percentile_approx`, `avg`, `max`), tuyệt đối không dùng `.over(Window.partitionBy())`
để "làm cho nó chạy theo dòng" — hãy tính ra rồi dùng như ngưỡng.

### 🐛 LỖI 6 — `persist()` làm job chết vì OOM (bài học quan trọng nhất về hiệu năng)

**Triệu chứng:** job dừng ở bước "Cache tầng sự kiện" sau 229 giây, log ghi:

```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.columnar.compression.DictionaryEncoding$Encoder.compress
  at org.apache.spark.storage.memory.MemoryStore.putIterator
```

Job bị abort hoàn toàn, mọi bảng Silver đều không được ghi.

**Nguyên nhân:** `events.persist(StorageLevel.MEMORY_AND_DISK)` trên 110 triệu dòng.
Spark 3.x cache DataFrame theo định dạng **columnar** (`DefaultCachedBatchSerializer`),
tức mỗi batch phải nén dictionary encoding từng cột string (`category_code`, `brand`,
`event_type`...). Dung lượng bộ nhớ tạm trong giai đoạn nén lớn hơn nhiều lần so với
kích thước dữ liệu thô. Driver heap chỉ cấp 4 GB → vỡ.

Điểm đáng chú ý: `MEMORY_AND_DISK` **không** cứu được, vì lỗi xảy ra ở bước **nén**
trong RAM, chưa tới bước spill xuống đĩa.

**Cách sửa — đổi chiến lược, không đổi tham số:**

| Cách | Trước (persist) | Sau (ghi đĩa trước) |
|---|---|---|
| Trung tâm | Cache `events` trong RAM/đĩa | Ghi `fact_event` ra Parquet **1 lần** rồi đọc lại |
| Tốn RAM | Cao (nén columnar) | Gần 0 (streaming ghi + đọc tuần tự) |
| Số lần quét | 1 lần cache + nhiều lần đọc cache | 1 lần ghi + 3 lần quét Parquet nén |
| Kiểm tra CL | trên biến trong RAM | **trên dữ liệu đã ghi xuống đĩa** (đúng hơn) |

Mã thay thế trong `Data_Silver.py` (bước 3b):
```python
fact_event.write.mode("overwrite").partitionBy("year", "month").parquet(
    os.path.join(SILVER, "fact_event")
)
events = spark.read.parquet(os.path.join(SILVER, "fact_event"))
```

**Bài học:**
1. `persist()` **không phải** lời giải pháp phổ quát cho "DataFrame bị dùng nhiều lần".
   Nó chỉ đúng khi tập dữ liệu **vừa vặn** với RAM. Với dữ liệu lớn hơn RAM,
   `persist` biến vấn đề "tính lại" thành vấn đề "không đủ RAM" — tệ hơn.
2. Với dữ liệu lớn, **ghi ra đĩa rồi đọc lại** thường là "materialization" rẻ hơn
   nhiều so với cache — Parquet nén đọc lại chỉ vài giây, còn cache columnar thì tốn
   RAM và có nguy cơ OOM ngay lập tức.
3. Ưu tiên kiểm tra chất lượng **trên dữ liệu đã ghi ra đĩa** — vừa nhanh hơn,
   vừa chứng minh được thứ thật sự đã lưu đúng.

---

---

## MỐC 3 — Giai đoạn 3: Gold Layer ✅ HOÀN THÀNH (24/10/2026)

### Vì sao cần Gold?
> Silver là dữ liệu đã chuẩn hóa nhưng vẫn ở dạng chi tiết. Dashboard cần **số liệu tổng hợp sẵn**,
> truy vấn trong mili-giây thay vì quét 110M dòng mỗi lần mở biểu đồ.

### 8 bảng KPI đã dựng

| Bảng | Câu hỏi nghiệp vụ trả lời |
|---|---|
| `kpi_daily_revenue` | Doanh thu mỗi ngày, so sánh 2 tháng |
| `kpi_category_performance` | Danh mục nào bán chạy, tăng trưởng ra sao (pivot `10_revenue`/`11_revenue`) |
| `kpi_conversion_funnel` | view → cart → purchase, tỉ lệ mỗi bước |
| `kpi_hourly_traffic` | Giờ nào cao điểm, theo category |
| `kpi_product_ranking` | Top sản phẩm bán chạy, tỉ lệ chuyển đổi |
| `kpi_user_segment` | Phân khúc RFM để chăm sóc khách hàng |
| `kpi_funnel_by_category` | Phễu chuyển đổi riêng theo từng danh mục |
| `kpi_revenue_reconciliation` | Đối chiếu doanh thu có/không phân loại (minh bạch) |

**Quy trình:** đọc `Data/silver/` → tổng hợp theo grain nghiệp vụ → ghi Parquet `Data/gold/`
→ in bảng tổng hợp để kiểm chứng ngay. Mỗi KPI có 1 bảng + 1 câu hỏi nghiệp vụ.

### Kết quả thực tế (đã kiểm chứng)
| KPI | Giá trị |
|---|---|
| Tổng doanh thu 2 kỳ | **505.126.293 USD** |
| Sự kiện xem / mua hàng | 104.331.840 / 1.659.703 |
| Tỉ lệ chuyển đổi | 1,591% |
| Giá trị đơn trung bình | 304,35 USD |
| Doanh thu từ sản phẩm không phân loại | 52.801.355 USD (10,4%) |
| Số sản phẩm / người dùng có mua | 226.974 / 697.470 |
| Thời gian chạy | 78,6 s |

**Phễu chuyển đổi:**
| Kỳ | views | carts | purchases | view→cart | cart→purchase | tổng |
|---|---|---|---|---|---|---|
| 10/2019 | 40.777.328 | 898.443 | 742.773 | 2,20% | 82,67% | 1,82% |
| 11/2019 | 63.554.512 | 2.930.007 | 916.930 | 4,61% | 31,29% | 1,44% |

**Insight đáng chú ý:** tháng 11 có lượt xem tăng 55,9% và lượt thêm giỏ tăng 226%, nhưng
tỉ lệ cart→purchase lại **giảm từ 82,67% xuống 31,29%** — tức là phễu bị "rò" ở bước
cuối. Số đơn hàng tăng nhưng chất lượng giỏ hàng giảm: tháng 11 nhiều lượt thêm giỏ
bỏ trống hơn, có thể do khuyến mãi/thanh toán hoặc hàng tồn kho.

**Phân khúc RFM (sau khi sửa lỗi chiều chấm điểm):**
| Phân khúc | Người dùng | Chi tiêu TB | Số lần mua TB |
|---|---|---|---|
| At Risk | 254.093 | 606,96 | 1,94 |
| Loyal | 194.732 | 231,27 | 1,39 |
| Regular | 93.196 | 152,75 | 1,34 |
| VIP | 90.918 | **2.214,28** | **6,16** |
| Big Spender | 64.531 | 1.399,50 | 3,27 |

### 🐛 LỖI 7 — Chấm điểm RFM bị đảo chiều (nhóm "VIP" lại là nhóm chi ít nhất)

**Triệu chứng:** lần chạy đầu, nhóm **VIP có `avg_spent` chỉ 94,70 USD và 1,00 lần mua** —
trong khi nhóm "New" lại chi 1.891 USD / 5,52 lần mua. Trái ngược hoàn toàn với nghĩa của từ.

**Nguyên nhân:** hàm `score_by_percentile(col, ascending=True)` có **hai nhánh bên trong
bị đảo ngược so với tên tham số**:
- nhánh `ascending=True` chấm "giá trị ≤ ngưỡng 0.2 → 5 điểm" (tức nhỏ = tốt)
- nhánh mặc định chấm "giá trị > ngưỡng 0.8 → 5 điểm" (tức lớn = tốt)

Nhưng tại call-site lại gọi `score_by_percentile("recency_days", ascending=False)` —
tức truyền `False` vào nhánh "nhỏ = tốt", khiến **recency bị chấm ngược** và
frequency/monetary cũng bị chấm ngược.

**Cách sửa:** đổi tên tham số theo **ngữ nghĩa** thay vì theo hướng sắp xếp:
```python
def score_by_percentile(col, higher_is_better=True):
    if higher_is_better:   # lớn = tốt -> 5 điểm
        ...  >= ngưỡng 0.8 -> 5
    else:                  # nhỏ = tốt -> 5 điểm
        ...  <= ngưỡng 0.2 -> 5
```
Call-site: `recency_days` → `higher_is_better=False`; `purchase_count`, `total_spent` → `True`.

**Bài học:** đặt tên tham số theo **ý nghĩa** ("giá trị lớn hơn có tốt hơn không") chứ
đừng theo hướng sắp xếp ("ascending"). Hướng sắp xếp rất dễ bị đảo khi copy/paste
và khiến lỗi này *không hiện ra* (vì code vẫn chạy, vẫn ra số, chỉ là số sai nghĩa).
Đây là loại lỗi nguy hiểm thứ hai sau lỗi `total_spent`.

---



### Vì sao cần Gold?
> Silver là dữ liệu đã chuẩn hóa nhưng vẫn ở dạng chi tiết. Dashboard cần **số liệu tổng hợp sẵn**,
> truy vấn trong mili-giây thay vì quét 110M dòng mỗi lần mở biểu đồ.

### Dự kiến các bảng KPI
| Bảng | Câu hỏi nghiệp vụ trả lời |
|---|---|
| `kpi_daily_revenue` | Doanh thu mỗi ngày, so sánh 2 tháng |
| `kpi_category_performance` | Danh mục nào bán chạy, tăng trưởng ra sao |
| `kpi_conversion_funnel` | view → cart → purchase, tỉ lệ mỗi bước |
| `kpi_hourly_traffic` | Giờ nào cao điểm, theo category |
| `kpi_product_ranking` | Top sản phẩm bán chạy, tỉ lệ chuyển đổi |
| `kpi_user_segment` | Phân khúc RFM để chăm sóc khách hàng |

---

## MỐC 4 — Giai đoạn 4: Dashboard & Trực quan hóa ✅ HOÀN THÀNH (24/10/2026)

### Quy trình đã làm
1. `Data_Dashboard.py` đọc 8 bảng Gold → dựng **7 biểu đồ PNG** bằng Matplotlib
   → xuất **7 bảng KPI dạng CSV** (để mở trực tiếp bằng Power BI / Excel).
2. `Data_Architecture.py` dựng **2 sơ đồ kiến trúc** PNG (DPI 200) cho đề tài mục 5.6.

### Vì sao một nguồn sự thật duy nhất?
> Thiết kế: `Gold → CSV → Dashboard`. Biểu đồ và bảng CSV đều đọc từ **cùng một tầng
> Gold**, không có chỗ nào hardcode số liệu. Khi pipeline đổi số, chạy lại
> `Data_Dashboard.py` là mọi thứ tự cập nhật — không thể quên, không thể sai.

### Kết quả
- 7 biểu đồ trong `Data/dashboard/`: doanh thu theo ngày, phễu chuyển đổi, top danh mục,
  lưu lượng theo giờ, phân khúc người dùng, top sản phẩm, đối chiếu doanh thu.
- 8 file CSV KPI (7 bảng + 1 file `tong_hop_kpi.csv` bảng KPI tổng hợp).
- Tổng cộng **9 ảnh PNG** (7 biểu đồ + 2 sơ đồ kiến trúc).

### Sơ đồ kiến trúc — `Data_Architecture.py` (bổ sung 24/10)
> Đề tài mục 5.6 yêu cầu "sơ đồ kiến trúc hoặc data-flow từ input đến output".
> Trước đó báo cáo chỉ có sơ đồ ASCII trong code block — không đạt yêu cầu minh họa.

**Quy trình:** vẽ bằng matplotlib (`FancyBboxPatch` + `FancyArrowPatch`) → xuất PNG DPI 200.
**Vì sao matplotlib chứ không dùng Graphviz/draw.io/PowerPoint:**
> - Chạy offline, chỉ cần `pip install matplotlib` → tái lập được trên máy khác.
> - Sơ đồ vẽ bằng code nên số liệu trong sơ đồ luôn khớp với pipeline, không phải sửa tay khi đổi số.
> - Graphviz không có sẵn trên máy; draw.io/PowerPoint tạo file nhị phân không kiểm soát được bằng code.

**Kết quả mong muốn:** 2 ảnh — sơ đồ kiến trúc end-to-end và sơ đồ mô hình sao Silver.
**Kết quả thực tế:**
| Ảnh | Nội dung |
|---|---|
| `00_kien_truc_tong_the.png` | 5 dải tầng (Sources → Bronze → Silver → Gold → Presentation), mũi tên ghi rõ script + thời gian chạy, vòng phản hồi `Data_Verify.py` |
| `01_mo_hinh_sao_silver.png` | `fact_event` ở giữa, 3 bảng chiều xung quanh, nhãn quan hệ khóa ngoại, khối kiểm soát chất lượng |

**Bài học:** một sơ đồ kiến trúc viết bằng code tốt hơn sơ đồ vẽ tay ở đúng điểm đó — số liệu
ghi trên sơ đồ luôn khớp với pipeline sau mỗi lần chạy lại, không bao giờ thành "ảnh chụp cũ".




### Vì sao?
> Đề tài mục 6.1 yêu cầu "Kết quả demo" và mục 5.7 yêu cầu minh chứng.
> Trong số quy trình được phép, `Analytics → Presentation` dùng Power BI/Tableau/Superset/Grafana.
> Với môi trường hiện tại (Windows + Python), matplotlib là lựa chọn khả thi nhất —
> nhưng cần kiểm chứng xem có nên đề xuất Power BI đọc trực tiếp Parquet (hỗ trợ tốt).

---

## GHI CHÚ KỸ THUẬT THƯỜNG GẶP

| Vấn đề | Nguyên nhân | Cách xử lý |
|---|---|---|
| `HADOOP_HOME and hadoop.home.dir are unset` | thiếu winutils.exe | copy vào `D:\Metadata\hadoop\bin` |
| `Cannot run program "python3"` | Spark JVM spawn worker bằng tên `python3` | copy `python.exe` → `python3.exe` trong venv |
| `Python worker exited unexpectedly` | Python 3.12 xóa `distutils` | dùng `.venv311` (Python 3.11.9) |
| `UnicodeEncodeError: charmap` | stdout Windows dùng cp1252 | `sys.stdout.reconfigure(encoding='utf-8')` |
| `UNABLE_TO_INFER_SCHEMA` | file tên bắt đầu bằng `_` | Hadoop bỏ qua, đổi tên file |
| `table df_oct not found` | `spark.sql()` không biết biến Python | dùng API DataFrame thay vì SQL string |
| `Not enough space to cache` | RAM 16GB, cache 110M dòng | chấp nhận spill xuống đĩa, dùng `approx_count_distinct` |
| event_time lệch 7 tiếng | session timezone theo giờ máy | `spark.sql.session.timeZone = UTC` |
| `OutOfMemoryError: Java heap space` khi `persist()` | Spark cache kiểu columnar cần > heap | ghi Parquet ra đĩa rồi đọc lại, **không** persist tập > RAM |
| Phân khúc RFM "VIP" có chi tiêu thấp nhất | tham số `ascending` bị đảo chiều trong 2 nhánh | đặt tên tham số theo ngữ nghĩa `higher_is_better` |

---

## NHÁNH LỆCH SO VỚI KẾ HOẠCH

| Ngày | Việc | Lý do |
|---|---|---|
| 24/10 | Sửa lại `Data_cleaning.py` chạy 707s thay vì dùng bản cũ | phát hiện lỗi múi giờ, Bronze cũ chứa dữ liệu sai |
| 24/10 | `Data_CDC.py` chạy lại lần 2 | lần 1 lỗi `spark.sql()` + quá chậm (308s) |
| 24/10 | `Data_Silver.py` viết lại luồng: ghi `fact_event` ra đĩa rồi đọc lại | `persist(MEMORY_AND_DISK)` gây OOM ở bước nén columnar |
| 24/10 | `Data_Gold.py` sửa chiều chấm điểm RFM | nhóm VIP bị đảo thành nhóm chi ít nhất |
| 24/10 | Chạy lại toàn bộ Silver → Verify → Gold → Dashboard → Word | lấy số liệu nhất quán sau 2 lần sửa lỗi logic |
| 24/10 | `BaoCao_Word.py` chuyển sang đọc số liệu từ CSV Gold | báo cáo phải tự cập nhật, không hardcode số |
| 24/10 | Tạo Canvas kiến trúc tổng thể | minh họa trực quan cho phần trình bày |
| 24/10 | Thêm `Data_Architecture.py` — vẽ 2 sơ đồ kiến trúc bằng matplotlib | đề tài mục 5.6 yêu cầu sơ đồ data-flow; trước đó chỉ có ASCII art |
| 25/10 | Dọn kho mã nguồn: xóa venv cũ 814 MB, 11 log trùng/lỗi, 4 script tạm, 2 báo cáo `.docx` cũ, staging 41 MB | kho tích tụ rác sau 4 ngày chạy lại pipeline; không phân biệt được log thành công với log lỗi |
| 25/10 | `requirements.txt`: bỏ `scikit-learn` | `scikit-learn` không file nào dùng |
| 25/10 | Chạy lại `Data_Verify.py` (95,4s) + `Data_collect.py` (345s) + `Data_Architecture.py` | xác nhận dọn dẹp không làm hỏng pipeline; 0 khóa mồ côi, 0 lệch múi giờ |
| 25/10 | **Xóa `BaoCao_Word.py` + file `.docx` + `python-docx`** | yêu cầu: tập trung code Big Data, bỏ lớp sinh báo cáo Word (không phải code Big Data) — ⚠️ phần xóa file `.docx` **sai**, đã đảo ở MỐC 7 |
| 25/10 | **Sửa 4 lỗi trực quan hóa** trong ảnh 01, 03, 06 và 2 sơ đồ kiến trúc | soi từng ảnh PNG phát hiện hộp chồng lấn, nhãn đè, nhãn trục nằm ngang — chi tiết ở MỐC 6 |
| 25/10 | `Data_collect.py` chuyển sang `setup_spark()`; `Data_Dashboard.py` bổ sung CSV `kpi_funnel_by_category` | nhất quán cấu hình Spark giữa 9 script; đủ 8 bảng Gold ra CSV |
| 25/10 | **MỐC 7**: khôi phục `BaoCao_TienDo_BigData.docx` từ VSS shadow copy; sửa OOM `Data_CDC.py`; chạy lại toàn bộ pipeline | xem MỐC 7 |
| 25/10 | **Sửa bug nghiêm trọng**: `Data_Silver.py` thiếu dòng `.write` cho `fact_event` → tầng Silver đọc lại dữ liệu cũ, pipeline không idempotent | phát hiện bằng cách so mtime file sau khi chạy lại; đã sửa + chạy lại Silver→Verify→Gold→Dashboard |
| 25/10 | Dọn sau khi chạy lại: xoá 6 log cũ (bản lỗi), đổi tên 8 log `*_run2` → `*_run`, xoá `__pycache__` + file tạm trong `%TEMP%` | giữ đúng 1 log/script; giữ lại `Data/` (output thật) và `raw` (nguồn gốc) |

---

## MỐC 5 — Dọn dẹp mã nguồn & tài liệu hóa (25/10/2026)

Ngày 25/10 không có thay đổi nghiệp vụ nào — **không sửa logic pipeline**. Toàn bộ
công việc là dọn dẹp kho mã nguồn và làm tài liệu khớp với code thực tế.

### Vì sao cần dọn

Sau 4 ngày chạy lại pipeline nhiều lần, kho dự án tích tụ nhiều thừa:
17 file log cho 10 script, 4 script điều tra tạm, 3 file báo cáo `.docx` cũ,
2 virtualenv (một cái không dùng được), và staging folder 41 MB.

Điều này gây 3 vấn đề thật:
1. **Không phân biệt được bản chạy thành công với bản chạy lỗi** — GV mở log thấy
   nhiều bản, không biết đâu là bản đúng.
2. **Tài liệu lệch với code** — tài liệu nhắc tên log đã xóa, `requirements.txt` liệt kê
   `scikit-learn` không dùng.
3. **Không có git** → xóa là không hoàn tác được, mọi thay đổi đều là rủi ro.

### Đã xóa

| Nhóm | Nội dung | Lý do |
|---|---|---|
| Venv cũ `.venv/` | 814 MB, Python 3.12 | PySpark 3.5.3 **không** chạy được với 3.12 (thiếu `distutils`). Giữ `.venv311` (652 MB) |
| Log trùng / log lỗi | 11 file: `clean_log.txt`, `cdc_log.txt`, `cdc_report.txt`, `silver_log.txt/2/3`, `verify_log2.txt`, `gold_log.txt/2/3`, `explore_silver.txt`, `word_log.txt` | Mỗi script chỉ giữ lại **1 bản chạy thành công** |
| Script điều tra tạm | `check_timezone.py`, `check_timezone2.py`, `explore_raw_recovery.py`, `explore_silver.py` | Chạy 1 lần để ra quyết định thiết kế, không được tài liệu nhắc tới, không nằm trong gói nộp |
| Báo cáo `.docx` cũ | `BaoCao_TienDo_BigData_20261024.docx`, `..._20261012_0108.docx` | Đã diff từng đoạn: 53/27 đoạn "chưa có ở bản hiện tại" đều chỉ là chi tiết môi trường, đã nằm trong bảng "Môi trường thực thi" |
| Rác khác | `_package/` (41 MB), `.idea/`, `Chương 3/4/5/` (rỗng), `__pycache__/` | Không dùng |

### Bài học rút ra: **đừng xóa file trước khi đọc tài liệu trỏ tới nó**

Bản sao lưu ở `_package/BigData_Ecommerce.zip` chứa `PACKAGE_README.md` và
`check_timezone.py` — 2 file **không tồn tại** trong thư mục gốc. Nếu xóa `_package/`
trước khi kiểm tra nội dung zip thì mất luôn 2 file đó.

Vì vậy quy trình dọn đặt là: **liệt kê → grep tham chiếu trong `.md`/`.py` → diff nội dung
file `.docx` → mới xóa**. Không bao giờ xóa theo cảm tính.

### Cải tiến sau khi dọn

| Việc | Chi tiết |
|---|---|
| Tên log chuẩn hoá | `<script>_run.log` cho mọi script. Đổi `collect_log.txt` → `collect_run.log`, `verify_log.txt` → `verify_run.log` |
| `requirements.txt` | Bỏ `scikit-learn` (không file `.py` nào dùng) |
| `README.md` | Thêm mục 10 (thứ tự đọc code), mục 11 (output đầy đủ), mục 12 (log thực thi), sơ đồ luồng dữ liệu 3.1 |
| `PHAN_1_QUY_TRINH_HUONG_DAN.md` | Cập nhật tên log, lệnh `pip install -r requirements.txt`, bỏ `scikit-learn` khỏi danh sách thư viện |
| Kiểm chứng lại sau dọn | `Data_Architecture.py` ✅ sinh 2 ảnh · `Data_Verify.py` ✅ chạy lại 95,4s — 0 khóa mồ côi, 0 dòng lệch múi giờ · `Data_collect.py` ✅ chạy lại 345s — 67.501.979 dòng, NULL khớp EDA cũ · `py_compile` cả 10 script ✅ không lỗi |

### Cảnh báo: **không có git repo**

`Test-Path .git` → False. Mọi lần xóa trước đây đều là không thể hoàn tác. Việc đầu tiên
nên làm cho đồ án: `git init` + commit toàn bộ mã nguồn (loại trừ `Data/raw/`,
`.venv311/`, `Data/bronze|silver|gold/` — quá nặng cho git) để có bản sao an toàn.

### 2 điểm còn tồn đọng — ĐÃ XỬ LÝ (25/10)

| # | Vấn đề | Vị trí | Mức độ ưu tiên | Trạng thái |
|---|---|---|---|---|
| 1 | `Data_collect.py` tạo `SparkSession` trực tiếp, **không** dùng `setup_spark()` → thiếu `session.timeZone = UTC` và không có fix `python3.exe` | `Data_collect.py` | Trung bình | ✅ **Đã sửa** — chuyển sang `setup_spark("Data_Collect", master="local[*]", shuffle_partitions=8, driver_memory="4g")`. Xoá session cũ tạo trực tiếp (nếu giữ cả hai sẽ xung đột). Chạy lại ✅ — NULL khớp EDA cũ |
| 2 | `Data_Dashboard.py` export **7** bảng CSV nhưng có **8** bảng Gold — thiếu `kpi_funnel_by_category` | `Data_Dashboard.py` | Thấp | ✅ **Đã sửa** — thêm `"kpi_funnel_by_category"` vào danh sách export. Nay đủ **8 bảng Gold + `tong_hop_kpi.csv` = 9 CSV** |

> Cả 2 điểm đã được xử lý trong MỐC 6 và kiểm chứng lại bằng cách chạy lại pipeline
> từ đầu — xem mục "Kiểm chứng sau khi sửa" bên dưới.

---

## MỐC 6 — Loại bỏ lớp báo cáo Word & sửa lỗi trực quan hóa (25/10/2026)

> ⚠️ **Quyết định trong MỐC 6 đã được sửa ở MỐC 7**: xóa `BaoCao_Word.py` thì giữ,
> nhưng **file `.docx` phải giữ lại** (đã khôi phục). Xem MỐC 7.

### Quyết định (đã sửa ở MỐC 7): xóa `BaoCao_Word.py`, **giữ** file `.docx`

Yêu cầu: *"file code báo cáo thì giữ làm gì, xóa đi, tập trung code big data"*.
(Sau đó bổ sung: *"file doc vẫn để lại nha"* → xem MỐC 7.)

| Đã xóa | Lý do |
|---|---|
| `BaoCao_Word.py` (~640 dòng) | Sinh báo cáo `.docx` — **không phải code Big Data**. Đề tài mục 5.4/5.6 yêu cầu trực quan hóa và minh chứng, không yêu cầu code sinh Word. `python-docx` chỉ dùng cho việc này |
| ~~`BaoCao_TienDo_BigData.docx` (1,4 MB)~~ | ~~Sản phẩm của script trên~~ → **KHÔNG xóa** — đây là sản phẩm bàn giao, đã khôi phục ở MỐC 7 |
| `python-docx` khỏi `requirements.txt` | Không còn script nào dùng |

**Quy trình pipeline còn lại vẫn đầy đủ và đúng trình tự:**
`collect → cleaning → CDC → Silver → Verify → Gold → Dashboard (+ Architecture)`

Bảng KPI vẫn xuất ra CSV ở `Data/dashboard/` (mở bằng Excel / import Power BI),
9 ảnh PNG vẫn đầy đủ, và số liệu trong báo cáo giờ đến trực tiếp từ tầng Gold
thay vì qua bước trung gian sinh Word.

### 3 lỗi trực quan hóa đã tìm ra và sửa

Kiểm tra lại **toàn bộ 9 ảnh PNG** bằng cách đọc từng ảnh, không chỉ nhìn code.
Phát hiện 3 lỗi thật — cả 3 đều là lỗi **hiển thị**, không phải lỗi tính toán số liệu.

#### Lỗi 1 — Ảnh `01_mo_hinh_sao_silver.png`: hộp chồng lấn nhau

**Triệu chứng:** `fact_event` đè lên `dim_product` và `dim_user`; nội dung 2 hộp bị che.

**Nguyên nhân:** toạ độ cũ —
`fact_event` x=33–67 (rộng 34) trong khi `dim_product` x=14–40 và `dim_user` x=62–88.
Vùng chồng: x 33–40 và x 62–67.

**Sửa:** tính lại toạ độ để 4 hộp **rời nhau hoàn toàn**:
- `fact_event`: x 36.5–63.5, y 33–60 (giữa)
- `dim_product`: x 4–31, y 52–79 · `dim_user`: x 69–96, y 52–79 (trên)
- `dim_category`: x 4–31, y 4–31 (dưới trái) · kiểm soát chất lượng: x 69–96, y 4–31 (dưới phải)

Mũi tên khóa ngoại cũng viết lại: từ 3 đường thẳng ngắn (chồng hộp) → 3 đường cong
`arc3` nối từ mép hộp chiều vào `fact_event`, nhãn `product_id` / `user_id` /
`category_code` đặt cạnh mũi tên.

#### Lỗi 2 — Ảnh `00_kien_truc_tong_the.png`: nhãn đè lên hộp KPI

**Triệu chứng:** nhãn trắng viền xanh `Data_Verify.py (57s)` đè lên hộp
`kpi_user_segment` và `kpi_revenue_reconciliation`.

**Nguyên nhân:** nhãn đặt ở `x=92.5, y=39.0` — đúng vị trí mà 2 hộp KPI hàng 2 chiếm.
Thêm nữa, nhãn `Data_Gold.py` (rất dài, ~70 đơn vị) nằm cùng khoảng trống ngang y≈36.

**Sửa:** đoán vị trí không hiệu quả (thử 3 lần vẫn đè) → dùng cách đo chắc chắn:
`ax.transData.transform()` để quy đổi toạ độ data → pixel. Kết luận: khoảng trống ngang
giữa Silver và Gold **đã bị nhãn `Data_Gold.py` chiếm hết**.

**Sửa cuối cùng:** đưa nhãn `Data_Verify.py` ra **cột trống bên phải** (x=98.6) và **xoay
dọc 90°**, mở rộng `xlim` từ 100 → 106 để có chỗ. Nhãn `Data_Gold.py` lùi sang `lx=48`
để tránh đè. Sửa luôn số liệu cũ trên sơ đồ (7 biểu đồ → 9 ảnh PNG, bỏ box "Báo cáo Word").

**Bài học:** khi sơ đồ có nhiều nhãn dài, **đo pixel bằng `transData.transform()`** thay vì
đoán tọa độ. Ba lần sửa "bằng mắt" đều sai.

#### Lỗi 3 — Biểu đồ 06 (và 03): nhãn trục nằm ngang, đè tick label

**Triệu chứng:** nhãn "Mã sản phẩm" nằm **ngang** ở góc trên trái, đè lên nhãn tick
đầu tiên (`P9672532224`).

**Nguyên nhân:** với `barh` + `invert_yaxis()`, matplotlib 3.11.2 đặt nhãn trục y với
`rotation=0, va='bottom'` → nằm ngang ở đỉnh trục. Biểu đồ 03 không lỗi vì dùng
`.iloc[::-1]` (đảo dữ liệu) thay vì `invert_yaxis()`.

**Sửa:** thêm helper `fix_horiz_axis(ax)` dùng chung:

```python
lbl = ax.yaxis.get_label()
lbl.set_rotation(90)
lbl.set_va("center")
lbl.set_ha("center")
```

Áp dụng cho biểu đồ 06 và cả 2 subplot của biểu đồ 03.

**Thêm:** biểu đồ 06 bỏ `ax.invert_yaxis()` (chính nó làm trục x nhảy lên **trên**, đè
tiêu đề) → thay bằng sắp xếp tăng dần rồi đảo thứ tự dữ liệu, giống biểu đồ 03.

#### Lỗi 4 — Biểu đồ 01: chú giải đè đường xu hướng

Nhãn chú giải đặt `loc="upper left"` nằm ngay trên đường "Tổng sự kiện".
Sửa: `loc="upper left", bbox_to_anchor=(1.005, 1.0)` → đặt chú giải **bên ngoài** vùng
dữ liệu, cạnh trục y phải.

### Ảnh nào KHÔNG bị lỗi?

Kiểm tra từng ảnh: `02_phieu_chuyen_doi.png`, `04_luu_luong_theo_gio.png`,
`05_phan_khung_nguoi_dung.png`, `07_doi_chieu_doanh_thu.png` — **không lỗi hiển thị**.
(Cả 4 đều không dùng `barh` với `invert_yaxis`, và chú giải không nằm trên vùng dữ liệu.)

### 2 điểm tồn đọng đã xử lý luôn

| # | Vấn đề | Cách sửa |
|---|---|---|
| 1 | `Data_collect.py` tạo `SparkSession` trực tiếp, không dùng `setup_spark()` | Chuyển sang `setup_spark("Data_Collect", ...)` → có `session.timeZone = UTC` + fix `python3.exe`, nhất quán với 8 script kia. **Chạy lại ✅** — 265.504 ms, kết quả khớp EDA cũ |
| 2 | `Data_Dashboard.py` export 7 CSV, thiếu `kpi_funnel_by_category` | Thêm vào danh sách → nay đủ **8 bảng Gold + tong_hop_kpi = 9 CSV** |

### Kiểm chứng sau khi sửa

| Hạng mục | Kết quả |
|---|---|
| `Data_Architecture.py` | ✅ 2 ảnh — đã soi từng ảnh, không còn chồng lấn |
| `Data_Dashboard.py` | ✅ 7 biểu đồ — ảnh 01, 03, 06 đã sửa xong |
| `Data_collect.py` | ✅ 265.504 ms — 67.501.979 dòng, NULL khớp EDA cũ |
| `Data/dashboard/` | ✅ 9 PNG · 9 CSV |
| `py_compile` cả 9 script | ✅ không lỗi |

---

## MỐC 7 — Khôi phục file `.docx` + chạy lại pipeline từ đầu (25/10/2026)

### Yêu cầu: giữ lại file `.docx`, chỉ bỏ script

> *"với file baocaso_word.py bỏ chưa, nhưng file doc vẫn để lại nha"*

MỐC 6 đã xóa **cả** `BaoCao_Word.py` **lẫn** `BaoCao_TienDo_BigData.docx` — phần xóa file
`.docx` là **quyết định sai**, vì đề tài vẫn cần một báo cáo Word để nộp. Chỉ nên xóa
script sinh báo cáo (không thuộc code Big Data), **giữ lại file kết quả**.

### Khôi phục như thế nào khi file đã xóa vĩnh viễn?

Xóa bằng `Remove-Item` (PowerShell) → **không qua Recycle Bin** → không khôi phục được
bằng thao tác thường. Đã thử lần lượt:

| Cách | Kết quả |
|---|---|
| Recycle Bin (`C:\$Recycle.Bin`) | ❌ Rỗng — `Remove-Item` không gửi vào đây |
| OneDrive / File History | ❌ Không có bản sao |
| Git | ❌ Repo ở `BigData/` **chưa có commit nào** (`master` rỗng) |
| **Volume Shadow Copy (VSS)** | ✅ **Thành công** |

Windows có sẵn 2 shadow copy của ổ C: (tạo lúc **23:38 24/10** và **01:26 25/10**),
tức là có ảnh chụp toàn bộ ổ đĩa **trước khi** file bị xóa.

**Cách truy cập shadow copy** (mất nhiều bước vì device path không phải lúc nào cũng đúng):

```powershell
# 1. Liệt kê shadow copy và lấy DeviceObject THẬT (không phải số thứ tự!)
Get-WmiObject Win32_ShadowCopy | Select-Object ID, DeviceObject
# → \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy10   (bản 23:38 24/10)
# → \\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy11   (bản 01:26 25/10)

# 2. Đọc trực tiếp qua .NET — symlink tạo bằng mklink KHÔNG liệt kê được
$p = "\\?\GLOBALROOT\Device\HarddiskVolumeShadowCopy11\Users\Admin\Documents\CaoHoc\BigData\E-commerce"
[System.IO.Directory]::GetFileSystemEntries($p)

# 3. Khôi phục
[System.IO.File]::Copy($src, $dst, $true)
```

**Bẫy đã mắc:** thử `mklink` + `dir` trả về rỗng (exit 0 nhưng không có nội dung) —
dễ tưởng shadow copy không chứa file. Số `HarddiskVolumeShadowCopy1`/`2` **không phải**
ID thật; phải lấy `DeviceObject` từ WMI. Đọc bằng `[System.IO.Directory]::GetFileSystemEntries()`
mới thấy được danh sách thật.

**Kết quả khôi phục:**

| File | Kích thước | Kiểm chứng |
|---|---|---|
| `BaoCao_TienDo_BigData.docx` | 1.398.156 bytes (1,4 MB) | ✅ 133 đoạn · 15 bảng · 9 ảnh nhúng — nguyên vẹn |

Kiểm tra nội dung sau khi khôi phục: đủ heading từ "1. Tổng quan đồ án" đến
"5.3. Phân khúc RFM", 15 bảng, 9 ảnh — khớp với bản gốc.

### Bài học

> **Xóa file trước khi chắc chắn là rủi ro thật.** Ở đây cứu được là nhờ có sẵn
> VSS shadow copy của Windows — thứ mà mình không hề biết là có. Nhưng đó là may mắn,
> không phải phương pháp. Nếu không có shadow copy thì file đã mất vĩnh viễn.
>
> Quy tắc từ nay: **xoá file kết quả → kiểm tra còn dùng không → nhưng file báo cáo
> cuối cùng luôn để lại.** Script sinh báo cáo thì bỏ được (là code không thuộc đề tài),
> còn file `.docx` là **sản phẩm bàn giao** — phải có.

### Chạy lại toàn bộ pipeline từ đầu — đánh giá sau khi phân tích

Vì đã sửa 2 điểm tồn đọng (`setup_spark()` trong `Data_collect.py`, CSV
`kpi_funnel_by_category`) nên chạy lại **toàn bộ** chuỗi để chắc chắn không hỏng gì,
không chỉ chạy lại script đã sửa.

| Bước | Script | Thời gian | Kết quả |
|---|---|---|---|
| 1 | `Data_collect.py` | ~320 s | ✅ Nov 67.501.979 · Oct 42.448.764 dòng; NULL `category_code` 21.898.171 · `brand` 9.218.235 · `user_session` 10 — **khớp EDA cũ** |
| 2 | `Data_cleaning.py` | **775,5 s** | ✅ `oct_2019.parquet` 86 file / 1,62 GB · `nov_2019.parquet` 136 file / 2,73 GB — **khớp** |
| 3 | `Data_CDC.py` | **1015,3 s** | ⚠️ Lần 1 **OOM** → đã sửa → ✅ `product_cdc` 10 file/8,47 MB · `category_cdc` 2 file/9,54 KB · `user_cdc` 10 file/23,72 MB |
| 4 | `Data_Silver.py` | **551,3 s** | ✅ `fact_event` 109.819.993 sự kiện + 3 dimension |
| 5 | `Data_Verify.py` | **48,1 s** | ✅ 0 dòng lệch `hour`/`day`/`event_date` → không lệch múi giờ |
| 6 | `Data_Gold.py` | **74,6 s** | ✅ 8 bảng KPI · doanh thu 10/2019 = 229.933.213 USD, 11/2019 = 275.193.081 USD |
| 7 | `Data_Dashboard.py` | **61,2 s** | ✅ 9 PNG + **9 CSV** (đủ 8 bảng Gold + `tong_hop_kpi`) · tỉ lệ chuyển đổi 1,591% · doanh thu 505.126.293 USD |
| 8 | `Data_Architecture.py` | ~2 s | ✅ 2 sơ đồ kiến trúc |
| 9 | *(sinh lại `.docx`)* | ~3 s | ✅ 9 ảnh trong báo cáo **khớp 100%** với PNG hiện tại |

**Đánh giá sau khi phân tích:**

- `setup_spark()` trong `Data_collect.py` **không gây hồi quy**: số dòng và số NULL
  khớp **chính xác** với lần chạy trước khi sửa. Điều này xác nhận `session.timeZone = UTC`
  chỉ **chuẩn hoá** cách parse timestamp chứ không đổi dữ liệu.
- Dải thời gian sau cleaning vẫn là `2019-10-01 00:00:00 → 2019-11-30 23:59:59` →
  không có chênh lệch 7 tiếng nào. `Data_Verify.py` xác nhận **0 dòng lệch**.
- Thời gian `Data_cleaning.py` hơi tăng (707 s → 775,5 s) — do chạy song song với các
  tiến trình nền còn sót lại từ trước, không phải do thay đổi code. Tương thích.
- Việc thêm `kpi_funnel_by_category` vào export CSV **không ảnh hưởng** 7 bảng cũ —
  chỉ thêm 1 file, tổng 9 CSV. Đã kiểm chứng `kpi_funnel_by_category.csv` có 2 KB,
  không rỗng.
- **Phát hiện thêm 1 lỗi thật** ở `Data_CDC.py` (OOM) mà lần chạy trước chưa gặp —
  xem mục bên dưới. Đã sửa và chạy lại thành công.
- Ảnh trong `.docx` khôi phục được **2/9 ảnh đã cũ** (bản trước MỐC 6 sửa chồng lấn).
  Đã sinh lại `.docx` từ script shadow + 9 PNG hiện tại → giờ **9/9 khớp 100%**
  (đã verify bằng MD5 của `word/media/*.png` so với file PNG trong `Data/dashboard/`).

### Quyết định cuối về lớp báo cáo

| | Trạng thái | Lý do |
|---|---|---|
| `BaoCao_Word.py` | ❌ **Đã xóa** | Không thuộc code Big Data — đề tài chấm phần xử lý dữ liệu, không chấm code sinh Word |
| `BaoCao_TienDo_BigData.docx` | ✅ **Giữ lại** (1.428 KB, 133 đoạn, 15 bảng, 9 ảnh) | Đây là **sản phẩm bàn giao** (báo cáo Word để nộp) |

`python-docx` vẫn nằm trong `.venv311` (không xóa thư viện đã cài) nhưng đã bỏ khỏi
`requirements.txt` vì không còn script nào trong repo dùng tới.

> **Lưu ý:** file `.docx` **không còn script sinh lại trong repo** (đúng theo yêu cầu
> bỏ `BaoCao_Word.py`). Nếu cần chỉnh báo cáo về sau thì phải sửa trực tiếp file
> `.docx` bằng Word/LibreOffice, hoặc lấy lại script từ shadow copy (đã xoá link).
> Bản script đã dùng để sinh lại nằm ở `%TEMP%\gen_docx\gen.py` — **đã xoá sau khi
> dùng**, nhưng nội dung gần như nguyên vẹn nằm trong shadow copy `HarddiskVolumeShadowCopy11`
> (còn tồn tại đến khi Windows dọn dẹp).

### ⚠️ Lỗi phát hiện khi chạy lại: `Data_CDC.py` OOM (sửa trong MỐC 7)

Chạy lại `Data_CDC.py` **lần đầu đã FAIL** ở Stage 6 (~147s):

```
ERROR Executor: Exception in task 29.0 in stage 6.0 (TID 74)
java.lang.OutOfMemoryError: Java heap space
→ TaskKilledException → py4j.protocol.Py4JError: py4j does not exist in the JVM
```

**Chẩn đoán:**

| Mắt xích | Chi tiết |
|---|---|
| Máy | 16 GB RAM, **chỉ còn 3,4 GB free** (WSL `vmmemWSL` giữ 8 GB, PyCharm 1,5 GB, Docker 0,7 GB) |
| Spark | `local[*]` = **16 task song song**, driver heap `4g` |
| Code | `df_oct.cache()` + `df_nov.cache()` trên **110 triệu dòng × 9 cột** |

`.cache()` = `StorageLevel.MEMORY_ONLY`. Khi phần dữ liệu không vừa RAM, Spark đổ xuống
disk **nhưng vẫn phải giữ block trong heap để serialize** → vượt 4 GB → OOM.

**Cách sửa (3 thay đổi):**

```python
from pyspark.storagelevel import StorageLevel

spark = setup_spark("Data_CDC", master="local[4]", shuffle_partitions=8,
                    driver_memory="6g", log_level="WARN")
# ...
df_oct = df_oct.persist(StorageLevel.MEMORY_AND_DISK)
df_nov = df_nov.persist(StorageLevel.MEMORY_AND_DISK)
```

1. **`MEMORY_AND_DISK` thay cho `.cache()`** — phần vừa RAM giữ trong RAM, phần dư ghi
   thẳng xuống disk, không bao giờ phải chứa toàn bộ block trong heap. Đây đúng là
   bài học đã viết ở `Data_Silver.py` (dòng 138–152) — **CDC chưa áp dụng**.
2. **`local[4]` thay cho `local[*]`** — 16 task song song trên máy còn 3,4 GB RAM free là
   quá nhiều; 4 task vừa đủ mà vẫn dùng hết 16 core qua 4 task × 4 luồng.
3. **`driver_memory` 4g → 6g** — chỉ tăng vừa đủ, không lên quá 8g để tránh tranh chấp
   với WSL/Docker.

**Kết quả sau sửa:** ✅ **1015,3 s** (cũ: 618s) — chậm hơn vì 4 task thay vì 16, nhưng
**không OOM** và ra đủ 3 bảng: `product_cdc.parquet` (10 file, 8,47 MB),
`category_cdc.parquet` (2 file, 9,54 KB), `user_cdc.parquet` (10 file, 23,72 MB).

> **Ghi chú:** `spark.driver.memory` đặt trong `SparkSession.builder.config()` **có
> giá trị trong conf** nhưng heap JVM thực tế do launcher quyết định lúc spawn
> (đã kiểm chứng: process java có `-Xmx4g`). Muốn đổi heap phải set **trước** khi
> tạo session qua `PYSPARK_SUBMIT_ARGS` / `SPARK_DRIVER_MEMORY`, không phải qua builder.

---

## 🐛 BUG NGHIÊM TRỌNG — `Data_Silver.py` KHÔNG BAO GIỜ GHI `fact_event` (25/10/2026)

### Phát hiện như thế nào?

Sau khi chạy lại pipeline, tôi liệt kê file trong `Data/` và lọc theo mtime để tìm
file nào **cũ hơn lần chạy vừa rồi**. Kết quả:

```
Data\silver\fact_event\year=2019\month=10\part-*.parquet   10/24/2026 8:47 AM  ← CŨ
Data\silver\fact_event\year=2019\month=11\part-*.parquet   10/24/2026 8:48 AM  ← CŨ
```

16 file `fact_event` **giữ nguyên mtime từ 24/10**, trong khi `dim_product`,
`dim_user`, `dim_category`, `gold/*`, `dashboard/*` đều mới. Nghĩa là `Data_Silver.py`
chạy hôm nay **không hề ghi lại** `fact_event`.

### Nguyên nhân — dòng `.write` bị THIẾU

`Data_Silver.py` bước 3b (dòng ~152–166):

```python
log("Ghi fact_event ra đĩa (thay cho cache trong RAM)...")

fact_event = events.select(...)          # ← chỉ SELECT, không WRITE!

log("fact_event đã ghi xong. Đọc lại từ đĩa để dựng chiều...")

events = spark.read.parquet(os.path.join(SILVER, "fact_event"))   # ← đọc lại file CŨ
```

Có `select()` nhưng **không có `.write`**. Log in ra "fact_event đã ghi xong" — sai.
Dòng `log("Ghi fact_event ra đĩa...")` chỉ mất 0,6 s (177,7 s → 178,3 s), quá nhanh
để ghi 3,9 GB — đã là dấu hiệu.

Đã đối chiếu với bản shadow copy 24/10: **lỗi có sẵn từ đầu**, không phải do
sửa đổi gì hôm nay. Nghĩa là `fact_event` trên đĩa là kết quả của một lần chạy
nào đó trước đó, và mọi lần chạy sau đều **đọc lại y hệt bản cũ**.

### Hậu quả

1. **Pipeline không idempotent.** Đổi `Data_cleaning.py` hoặc nguồn dữ liệu thì tầng
   Silver vẫn giữ `fact_event` cũ → `Data_Gold.py` đọc `fact_event` cũ → KPI sai.
2. **`Data_Verify.py` kiểm tra dữ liệu cũ.** Nó đọc `Data/silver/fact_event` từ đĩa và
   báo "0 khóa mồ côi" — nhưng đó là bản cũ, không phải bản vừa sinh ra.
3. Lỗi này **giấu được** vì số liệu khớp EDA cũ — người không kiểm tra mtime file
   sẽ tưởng mọi thứ ổn.

### Cách sửa

```python
fact_event.write.mode("overwrite").partitionBy("year", "month").parquet(
    os.path.join(SILVER, "fact_event")
)
```

- `mode("overwrite")` — xoá partition cũ rồi ghi mới, không dính dữ liệu stale.
- `partitionBy("year", "month")` — giữ nguyên cấu trúc partition như lần ghi đầu
  (file cũ có `year=2019/month=10` và `year=2019/month=11`).

### Kết quả sau khi sửa

Đã chạy lại chuỗi **Silver → Verify → Gold → Dashboard → Architecture**:

| Bước | Thời gian | Kết quả |
|---|---|---|
| `Data_Silver.py` | **1094,0 s** | ✅ `fact_event` **32 file, 3,91 GB** — mtime **mới** (18:19–18:20). 109.819.993 dòng · `dim_product` 206.876 · `dim_user` 5.316.649 · `dim_category` 130 |
| `Data_Verify.py` | 47,4 s | ✅ 0 dòng lệch `hour`/`day`/`event_date` · 0 khóa mồ côi |
| `Data_Gold.py` | 77,5 s | ✅ doanh thu 10/2019 = 229.933.213 USD, 11/2019 = 275.193.081 USD — **khớp hệt** lần trước |
| `Data_Dashboard.py` | 60,0 s | ✅ 9 PNG + 9 CSV · tỉ lệ chuyển đổi 1,591% · doanh thu 505.126.293 USD |
| `Data_Architecture.py` | ~2 s | ✅ 2 sơ đồ |

**Đáng chú ý:** thời gian `Data_Silver.py` tăng từ **551 s → 1094 s** (gần gấp đôi).
Chính là vì lần này **thực sự ghi** 3,91 GB `fact_event` xuống đĩa, trong khi lần
trước bỏ qua bước ghi (chỉ 0,6 s). Số liệu KPI **không đổi** → xác nhận dữ liệu cũ
và dữ liệu mới là cùng một nguồn, chỉ là lần trước pipeline không ghi ra được.

### Dọn dẹp sau khi chạy lại (theo yêu cầu "lược bỏ bớt, không cần thì xoá")

| Mục | Quyết định | Lý do |
|---|---|---|
| 6 log cũ (`cdc_run.log`, `silver_run.log`, … bản lỗi/trước sửa) | ❌ **Xoá** | Ghi lại log lỗi OOM + log trước khi sửa `fact_event` → gây hiểu nhầm |
| 8 log mới (`*_run2.log`) | ✅ **Đổi tên** về `*_run.log` | Giữ đúng 1 bản log cho mỗi script, tên nhất quán |
| `__pycache__/` | ❌ **Xoá** | Byte-code sinh tự động, Python tạo lại khi cần |
| `%TEMP%\BaoCao_backup_restored.docx`, `Silver_shadow.py`, `BaoCao_Word_shadow.py` | ❌ **Xoá** | File tạm trong quá trình khôi phục, đã dùng xong |
| `Data/bronze/cdc/*_csv/` (~152 MB) | ✅ **Giữ** | `README.md` mô tả là "bản CSV để mở bằng Excel"; `Data_CDC.py` sinh ra có chủ đích |
| `Data/raw/*.csv` (14 GB) | ✅ **Giữ** | Nguồn dữ liệu gốc, không thể tạo lại |
| Toàn bộ `Data/bronze`, `silver`, `gold`, `dashboard` | ✅ **Giữ** | Output thật của pipeline, đều mới từ lần chạy cuối |

**Đã kiểm tra lại toàn bộ `Data/`**: không còn file nào có mtime cũ hơn lần chạy vừa rồi
(trừ `raw` là nguồn gốc) — tức không còn partition mồ côi hay dữ liệu stale nào.

### Bài học

> **Log in ra "đã ghi xong" không phải bằng chứng đã ghi.** Phải kiểm chứng bằng
> **mtime của file trên đĩa**, không tin vào thông báo của chính script.
>
> Cách phát hiện tổng quát (nên làm sau mỗi lần chạy lại): liệt kê file trong
> `Data/` và tìm file nào **cũ hơn thời điểm bắt đầu chạy**. File nào không đổi
> mtime dù script chạy qua là ứng viên đáng ngờ ngay.

### Bài học thêm: chi phí của việc "bỏ qua bước ghi"

`Data_Silver.py` chạy nhanh hơn 543 s (551 s vs 1094 s) **chỉ vì bỏ qua bước ghi
3,91 GB**. Nếu đo hiệu năng mà không biết bước nào thực sự chạy thì sẽ tối ưu
nhầm — tưởng pipeline nhanh vì code tốt, thực ra là pipeline **không làm việc**.

---

## TỔNG KẾT TRẠNG THÁI (25/10/2026)

| Giai đoạn | Script | Trạng thái | Thời gian (chạy lại 25/10) |
|---|---|---|---|
| 1.1–1.2 Thu thập + EDA | `Data_collect.py` | ✅ | ~320s (đã chuyển sang `setup_spark()`) |
| 1.3 Làm sạch → Bronze | `Data_cleaning.py` | ✅ | 775,5s |
| 1.4 CDC | `Data_CDC.py` | ✅ | **1015,3s** (đã sửa OOM) |
| 2 Tách chiều → Silver | `Data_Silver.py` | ✅ | **1094,0s** (đã sửa bug không ghi `fact_event`) |
| 2 Kiểm chứng | `Data_Verify.py` | ✅ | 47,4s — 0 dòng lệch múi giờ, 0 khóa mồ côi |
| 3 KPI → Gold | `Data_Gold.py` | ✅ | 77,5s |
| 4 Biểu đồ + CSV | `Data_Dashboard.py` | ✅ | 60,0s — 9 PNG + **9 CSV** (đủ 8 Gold + `tong_hop_kpi`) |
| 4 Sơ đồ kiến trúc | `Data_Architecture.py` | ✅ | ~2s |
| Báo cáo Word | `BaoCao_TienDo_BigData.docx` | ✅ | 1.428 KB · 133 đoạn · 15 bảng · 9 ảnh khớp 100% |
| Dọn dẹp + tài liệu | — | ✅ | 25/10/2026 |

**Toàn bộ pipeline chạy xong end-to-end**, kết quả kiểm chứng được:

- Bronze 109.950.743 dòng → Silver 109.819.993 dòng (bỏ 130.750 dòng trùng)
- 0 khóa mồ côi trên cả 3 quan hệ — mô hình sao toàn vẹn
- 8 bảng KPI ở tầng Gold · **9 ảnh PNG** (7 biểu đồ + 2 sơ đồ) · **9 file CSV**
- **13 lỗi đã xử lý**: 7 lỗi pipeline (mục 1–7) + 4 lỗi trực quan hóa (MỐC 6)
  + 1 lỗi OOM ở CDC + **1 bug nghiêm trọng `Data_Silver.py` không ghi `fact_event`**
- Kho mã nguồn gọn, **chỉ còn code Big Data**: 9 script · 3 tài liệu `.md`
  + 1 file báo cáo `.docx` (sản phẩm bàn giao) + 8 file log (đúng 1 log/script)

### Nếu cần mở rộng tiếp
| Hướng | Mức độ | Ghi chú |
|---|---|---|
| Kafka Streaming | Trung bình | Cần Docker + Kafka, hoặc chứng minh bằng mô phỏng file stream |
| Power BI trực tiếp | Thấp | CSV trong `Data/dashboard/` đã sẵn sàng import |
| Spark trên cụm | Cao | Yêu cầu môi trường cụm, hiện chạy local[*] |
| Dự đoán churn (ML) | Trung bình | Có thể dùng `kpi_user_segment` làm nhãn đầu ra |
| Dự án "mẫu" cho người khác | Trung bình | ~85% tài liệu tái sử dụng được, xem mục 10–11 `README.md` |
| Git remote | ✅ 27/10/2026 | `origin` = `https://github.com/nghiatnsimed-AI/BigData.git` |

---

## MỐC 8 — Đưa mã lên Git, không đẩy data (27/10/2026)

**Vấn đề:** GitHub Desktop đang stage ~11.600 file (venv, `.pyc`, `.idea`, CSV/Parquet).
Push một lúc sẽ fail vì file > 100 MB và làm repo hàng chục GB.

**Cách làm:**

1. Viết `.gitignore` ở gốc `BigData` — loại `Data/raw|bronze|silver|gold`, `.venv311`,
   `__pycache__`, `.idea`, hai CSV ranking/segment lớn.
2. Commit **theo đợt nhỏ** (gitignore → code → tài liệu → PNG/log/docx).
3. Giữ `.gitkeep` trong các thư mục data để clone vẫn thấy cấu trúc Lakehouse.
4. Ghi chú vào `README.md` (mục 13) và `PHAN_1` mục 4.11.

**Cố ý không lên Git:** raw 14 GB, Parquet ~8,8 GB, venv 650 MB,
`kpi_user_segment.csv` 42 MB, `kpi_product_ranking.csv` 19 MB.
Người khác tái tạo bằng Kaggle + chạy pipeline.

---

## MỐC 9 — README: cài Java, môi trường, chạy tuần tự (27/10/2026)

**Vì sao:** README cũ chỉ 4 dòng comment (`Đặt JAVA_HOME = ...`) — người clone không
biết tải JDK ở đâu, đặt biến môi trường thế nào, vì sao cần `winutils`/`python3.exe`,
và chạy 8 script theo thứ tự nào thì được coi là xong.

**Đã làm:** viết lại mục 1 và mục 3 của `README.md`.

- Mục 1: JDK 17 (Adoptium, `JAVA_HOME` + `Path`, lệnh kiểm tra), winutils +
  `hadoop.dll` (đường dẫn khớp `config.py`), Python 3.11 venv, `requirements.txt`,
  copy `python3.exe`, tải CSV Kaggle, checklist `Test-Path`.
- Mục 3: bảng 8 bước (lệnh, thời gian, input/output, tiêu chí thành công), lệnh
  từng script có `Tee-Object` log, chuỗi PowerShell khi đã chắc môi trường.

Không đổi mã pipeline. Chi tiết từng phép xử lý vẫn nằm ở `PHAN_1_QUY_TRINH_HUONG_DAN.md`.

**Bổ sung:** cùng nội dung cài Java / môi trường / chạy tuần tự được ghi vào
`BigData/README.md` (gốc repo, nơi GitHub hiện khi clone). Git remote gắn ở thư mục
gốc, không phải `E-commerce/` — người clone đọc file ngoài trước. File
`E-commerce/README.md` vẫn giữ bản đầy đủ (KPI, kiến trúc, thứ tự đọc code).

---

## MỐC 10 — Kiểm tra trạng thái Git + dọn `.git` 7,9 GB (27/10/2026)

**Nghi vấn của user:** "hình như chưa đẩy hết file lên git".

**Kết luận kiểm tra:** **đã push đủ**. `git status` = working tree clean,
`origin/master` = `e9e81a0` = local `HEAD`, `git rev-list --objects origin/master`
= 92 object. Clone thử sạch từ GitHub → 83 MB, đủ code + tài liệu + PNG + CSV.

**Vấn đề phát hiện được:** `.git` local nặng **7.916 MB** (7,9 GB) trong khi lịch
sử chỉ chứa ~80 MB blob thật. Nguyên nhân: commit `4850ac0` ("bronze") từng
stage các file CSV lớn của `Data/bronze/cdc/*_csv/` rồi bị revert/reset, tạo ra
**10.981 blob unreachable (11,2 GB)** trong object store. Commit này **chưa
từng** được push (`git merge-base --is-ancestor` → false), nên GitHub không bị
ảnh hưởng.

**Đã xử lý:**
1. `git reflog expire --expire=now --expire-unreachable=now --all`
2. `git gc --prune=now`
→ `.git` từ **7.916 MB → 38,7 MB**, pack 4 → 1 (35,7 MiB), 0 unreachable.
`git fsck` sạch, working tree clean, không mất file đang làm việc.

**Quyết định của user (27/10/2026):** giữ nguyên `.gitignore` — **không** đẩy
tầng Gold Parquet (~20 MB) lên Git, dù nhẹ. Người khác tự chạy `Data_Gold.py`.

**Tài liệu đã cập nhật cho khớp thực tế:**
- `BigData/README.md` mục 6 (mới): hướng dẫn gửi cho giảng viên — mã qua Git,
  dữ liệu gốc ~14 GB gửi riêng (khuyến nghị link Kaggle), kèm bảng "gửi kèm
  gì / dung lượng / giá trị".
- Sửa số liệu dung lượng: bronze 4,5 → 4,6 GB, silver 4,1 → 4,2 GB.
- `E-commerce/README.md` mục 13.1 (mới): bảng "cần gửi / cách / dung lượng".

---

## MỐC 11 — Xác nhận: chỉ 2 file CSV là đầu vào (27/10/2026)

**Câu hỏi của user:** "chỉ cần đưa 2 file CSV là được hả, chạy code sẽ tự sinh
ra mấy file còn lại hả?"

**Đã kiểm tra lại từng script** (grep mọi lệnh `read`/`write`):

| Script | Đọc | Tự sinh |
|---|---|---|
| `Data_collect.py` | `Data/raw/2019-Nov.csv` | — |
| `Data_cleaning.py` | cả 2 CSV | Bronze 2 thư mục Parquet |
| `Data_CDC.py` | Bronze | `bronze/cdc/` 3 Parquet + 3 CSV |
| `Data_Silver.py` | Bronze + CDC | `silver/` fact_event + 3 dim |
| `Data_Verify.py` | Silver | — |
| `Data_Gold.py` | Silver | `gold/` 8 bảng KPI |
| `Data_Dashboard.py` | Gold | `dashboard/` 9 PNG + 8 CSV |
| `Data_Architecture.py` | không đọc data | 2 PNG sơ đồ |

**Kết luận: đúng.** 2 CSV là toàn bộ đầu vào. Mọi thư mục đích được tạo tự động
bằng `os.makedirs(..., exist_ok=True)` (Gold, Silver, CDC, dashboard) → người
nhận chỉ cần có `Data/raw/` là chạy được.

**Lưu ý thêm:** `Data_Architecture.py` vẽ sơ đồ từ số liệu ghi cứng trong code,
không đọc dữ liệu → sơ đồ vẫn đúng dù chạy trên máy khác.

**Ghi:** `BigData/README.md` mục 5.1 (mới) + `E-commerce/README.md` mục 13.1.

---

## MỐC 12 — Xác nhận hành vi ghi đè + thứ tự phụ thuộc giữa các tầng (28/10/2026)

**Câu hỏi của user:** "chạy lại từ đầu thì có đè lên data cũ không; Silver chỉ
cần chạy CDC xong chạy Silver là tự tạo folder và file đúng không?"

**Đã grep toàn bộ `mode(` / `write.` / `makedirs` trong 8 script:**

1. **Ghi đè: đúng, sạch.** Mọi lệnh ghi đều `mode("overwrite")` —
   Bronze (`partitionBy(year, month)`), CDC (3 Parquet + 3 CSV), Silver (fact_event
   + 3 dim), Gold (8 bảng). Dashboard/Architecture dùng `savefig` / `to_csv` nên
   ghi đè file cùng tên. Kèm `partitionOverwriteMode = dynamic` trong `config.py`
   → Spark chỉ xoá partition bị ảnh hưởng.
   → **Chạy lại idempotent, không cần xoá thủ công.**

2. **Tự tạo thư mục: đúng.** `os.makedirs(..., exist_ok=True)` cho `silver/`,
   `bronze/cdc/`, `gold/`, `dashboard/`.

3. **Phát hiện quan trọng về thứ tự phụ thuộc.** `Data_Silver.py` **đọc**
   `bronze/cdc/product_cdc.parquet` và `user_cdc.parquet` (dòng 295–296) để gắn
   nhãn `change_type` / `user_status` vào bảng chiều → **bắt buộc chạy CDC trước
   Silver**. `Data_Gold.py` đọc `silver/`, `Data_Dashboard.py` đọc `gold/`.

4. **Cảnh báo về thứ tự xoá thư mục.** Xoá `bronze/` mà quên `bronze/cdc/` →
   `Data_Silver.py` **crash**. Xoá `silver/` mà giữ `gold/` → không lỗi nhưng
   `gold/` là dữ liệu cũ, không tái lập được. Nguyên tắc: **xoá tầng nào thì
   xoá luôn các tầng bên dưới**, và chạy lại từ tầng đó trở xuống.

**Ghi:** `BigData/README.md` thêm mục 5.1 (ghi đè), 5.2 (tự tạo thư mục),
5.3 (thứ tự phụ thuộc + bảng "muốn làm gì → chạy lại từ đâu"), đổi mục cũ
5.1 → 5.4. `E-commerce/README.md` mục 13.1 cập nhật tham chiếu 5.1 → 5.1–5.4.

---

## MỐC 13 — Script batch `run_all.ps1` / `run_all.py` (28/10/2026)

**Câu hỏi của user:** "chỉ cần chạy code là được hả?"

**Đã kiểm tra môi trường hiện tại — đã sẵn sàng 100%, không cần cài gì thêm:**

| Hạng mục | Trạng thái |
|---|---|
| Java | 17.0.12 LTS ✓ |
| `JAVA_HOME` | `D:\Metadata\jdk-17.0.12` ✓ |
| `HADOOP_HOME` | `D:\Metadata\hadoop` ✓ |
| `winutils.exe` + `hadoop.dll` | ✓ (có cả trong `$JAVA_HOME\bin`) |
| venv `.venv311` | Python 3.11.9 ✓ |
| `python3.exe` trong venv | ✓ |
| pyspark / pandas | 3.5.3 / 3.1.0 ✓ |
| 2 file CSV | 5,28 GB + 8,39 GB ✓ |
| Output hiện có | Silver 4 thư mục, Gold 8 bảng, Dashboard 9 PNG ✓ |

**Nhưng phát hiện 1 điểm bất tiện:** phải gõ 8 lệnh, đúng thứ tự, không được
song song (script sau đọc file trên đĩa do script trước ghi). Dễ sai, dễ mất
thời gian, và khi lỗi thì không biết dừng ở đâu.

**→ Đã tạo 2 script batch:**

1. `E-commerce/run_all.ps1` (PowerShell) — kiểm tra môi trường, chạy 8 bước
   tuần tự, ghi log `Data/logs/`, in thời gian từng bước, **dừng ngay** nếu 1
   bước fail. Tham số: `-From <n>` (chạy lại từ bước n), `-Only <tên>` (1 bước).
2. `E-commerce/run_all.py` (Python, cross-platform) — cùng logic, dùng
   `subprocess.Popen` + pipe để vừa hiện màn hình vừa ghi log. Tham số:
   `--from <n>`, `--only <tên>`.

**Đã kiểm thử cả 2** với `-Only architecture` → exit 0, sinh đúng 2 PNG.

**Quyết định thiết kế:** batch runner **dừng ngay** khi 1 bước fail, không
chạy tiếp — vì nếu chạy tiếp thì các bước sau sẽ đọc dữ liệu cũ và tạo ra
kết quả sai mà không báo lỗi. Đây là cùng nguyên tắc với mục 5.3 (thứ tự phụ
thuộc).

**Ghi:** `BigData/README.md` mục 5.0 (cách nhanh) + mục 6.4 (danh mục file).







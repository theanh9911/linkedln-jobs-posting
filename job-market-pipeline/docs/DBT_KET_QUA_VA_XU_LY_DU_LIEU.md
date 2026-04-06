# Kết quả chạy dbt & cách xử lý dữ liệu “lệch”

Tài liệu ghi lại **kết quả một lần chạy** `dbt build` (tham chiếu log), **ý nghĩa lỗi test**, và **cách dự án xử lý** trong SQL (staging / marts).

---

## 1. Tóm tắt lần chạy (log mẫu)

| Chỉ số | Giá trị |
|--------|---------|
| Models | 18 (staging + intermediate + marts) |
| Tests | 14 (data tests) |
| Thời gian | ~27–30 giây (tùy mạng / BQ) |
| Kết quả | **30 PASS**, **2 ERROR** (trước khi chỉnh code chuẩn hóa) |

**Models chạy thành công:** toàn bộ view staging, `int_job_postings_joined`, `dim_*`, `fct_job_skills`, incremental `fct_job_postings` (~123k dòng).

**2 test fail (phiên bản trước khi fix):**

| Test | Kết quả | Ý nghĩa |
|------|---------|---------|
| `accepted_values` trên `fct_job_postings.work_type` | **7 dòng** không thuộc tập giá trị cho phép | Trường `work_type` / `formatted_work_type` ở raw có giá trị **khác** với danh sách chuẩn (ví dụ: biến thể chữ, giá trị hiếm, hoặc không khớp `Full-time` / `Part-time` …). |
| `relationships` `company_id` → `dim_companies` | **1 dòng** | Job có `company_id` **không null** nhưng **không tồn tại** trong bảng công ty (dữ liệu mồ côi / scrape thiếu company). |

---

## 2. Dữ liệu đó được xử lý thế nào trong pipeline?

### 2.1. `work_type` (7 dòng lệch test)

**Vấn đề:** BI và test `accepted_values` cần một **tập giá trị cố định**:  
`Contract`, `Full-time`, `Part-time`, `Volunteer`, `Other`, `Internship`.

**Cách làm trong dự án:** Ở **`stg_job_postings`**, cột `work_type` được **chuẩn hóa** từ  
`trim(coalesce(formatted_work_type, work_type))` bằng `CASE`:

- Map các biến thể phổ biến (ví dụ `fulltime`, `f/t`, `temporary` …) về một trong các giá trị chuẩn ở trên.
- Giá trị **không nhận diện được** → gom vào **`Other`** (vẫn hợp lệ cho test và dashboard).

**Tư duy:** Không sửa tay từng dòng trong CSV; **quy tắc nằm trong SQL** để mọi lần load lại dữ liệu vẫn nhất quán.

---

### 2.2. `company_id` orphan (1 dòng lệch relationship)

**Vấn đề:** Có job trỏ tới `company_id` mà **không có** trong `companies` → vi phạm quan hệ fact–dimension nếu coi `company_id` là khóa ngoại bắt buộc.

**Cách làm trong dự án:** Ở **`fct_job_postings`**, sau `int_job_postings_joined`, join với **`dim_companies`**:

- Nếu `company_id` **có** trong dim → giữ nguyên.
- Nếu **không có** → gán **`company_id = NULL`** (tin vẫn giữ trong fact, nhưng không gắn được công ty trong mart).

**Tư duy:** Không xóa job; **làm rõ giới hạn dữ liệu** (thiếu profile công ty) thay vì giả định FK luôn đúng.

---

## 3. Việc bạn cần làm sau khi đổi logic SQL

1. Chạy lại pipeline (ưu tiên full refresh cho fact nếu cần đồng bộ lịch sử):

   ```bash
   uv run --env-file .env dbt build --project-dir job-market-pipeline/dbt --profiles-dir job-market-pipeline/dbt
   ```

2. Nếu incremental khiến bản ghi cũ chưa cập nhật `work_type` / `company_id` đã sửa:

   ```bash
   uv run --env-file .env dbt run --project-dir job-market-pipeline/dbt --profiles-dir job-market-pipeline/dbt --select fct_job_postings --full-refresh
   ```

---

## 4. Ghi chú về chất lượng dữ liệu nguồn (Kaggle / LinkedIn scrape)

| Hiện tượng | Mức độ | Hành động trong mart |
|------------|--------|----------------------|
| Thiếu `company_id` | ~1.7k dòng (đã biết từ lần test trước) | Giữ NULL; không ép `not_null` ở staging. |
| `company_id` có nhưng không có trong `companies` | Hiếm (ví dụ 1 dòng) | NULL hóa ở `fct_job_postings` khi không join được dim. |
| `work_type` không chuẩn | Vài dòng | Chuẩn hóa + gom “còn lại” → `Other`. |

---

## 5. Đề xuất bố cục dataset BigQuery (chuẩn gọn)

| Dataset | Vai trò |
|---------|---------|
| **`linkedin_raw`** (hoặc `*_raw`) | Chỉ load từ GCS; bảng “như nguồn”; ít người có quyền ghi. |
| **`linkedin_marts`** (hoặc `*_analytics`) | Toàn bộ output dbt: view `stg_*`, `int_*`, bảng `fct_*` / `dim_*` — **một dataset**, phân biệt bằng prefix tên bảng. |

Trong repo, `dbt_project.yml` **không** đặt `+schema` trùng `BQ_MARTS_DATASET` trên nhánh `marts` để tránh dataset dạng `linkedin_marts_linkedin_marts`. Sau khi đổi cấu hình, chạy lại `dbt build`; dataset cũ trùng tên có thể xóa tay trên console nếu không còn dùng.

*Nâng cao (khi team lớn):* thêm dataset `*_staging` tách riêng view trung gian — không bắt buộc cho dự án cá nhân.

---

*Tài liệu này có thể cập nhật sau mỗi lần bạn thay đổi test hoặc nguồn CSV.*

# Job Market Analytics Pipeline 🚀

Dự án này xây dựng một hệ thống phân tích thị trường việc làm từ bộ dữ liệu LinkedIn (124K+ tin tuyển dụng), sử dụng Python để ETL, BigQuery làm kho dữ liệu (Data Warehouse) và dbt để biến đổi dữ liệu.

## 🛠 Kiến Trúc Hệ Thống
1. **Extract**: Python (Pandas) đọc CSV -> Parquet (Parquet nhanh hơn CSV 10-20 lần khi truy vấn).
2. **Load**: Đẩy Parquet lên GCS -> BigQuery (Bảng `raw`).
3. **Transform**: dbt biến đổi dữ liệu sang bảng Star Schema (Bảng `marts`).
4. **Dashboard**: Looker Studio kết nối BigQuery để trực quan hóa.

## 📂 Cấu Trúc Thư Mục
```
job-market-pipeline/
├── scripts/
│   ├── extract.py    # CSV -> Parquet & Làm sạch sơ bộ
│   └── load.py       # GCS -> BigQuery (Partitioned)
├── dbt/              # Thư mục chứa dbt models
│   ├── models/
│   │   ├── staging/  # Làm sạch, chuẩn hóa dữ liệu
│   │   └── marts/    # Bảng Fact & Dim cho Dashboard
│   ├── dbt_project.yml
│   └── profiles.yml
├── requirements.txt
└── .env              # File cấu hình biến môi trường
```

## 🚀 Hướng Dẫn Cài Đặt (Setup)

### Bước 1: Chuẩn bị môi trường
1. Cài đặt các thư viện cần thiết bằng `uv`:
   ```bash
   uv pip install -r requirements.txt
   ```
2. Cập nhật thông tin trong file `.env`:
   - `GCP_PROJECT_ID`: ID dự án của bạn trên GCP.
   - `GCS_BUCKET_NAME`: Tên bucket bạn đã tạo trên GCS.

### Bước 2: Chạy Pipeline ETL (Ngày 1)
1. **Trích xuất và làm sạch dữ liệu**: Chuyển đổi CSV sang Parquet (giảm dung lượng đáng kể).
   ```bash
   python scripts/extract.py
   ```
2. **Tải dữ liệu lên GCP**: Đẩy file lên Storage và nạp vào BigQuery.
   ```bash
   python scripts/load.py
   ```

### Bước 3: Biến đổi dữ liệu với dbt (Ngày 1 & 2)
1. Di chuyển vào thư mục dbt:
   ```bash
   cd dbt
   ```
2. Kiểm tra kết nối:
   ```bash
   dbt debug
   ```
3. Chạy các model transformation:
   ```bash
   dbt run
   ```
4. Kiểm tra chất lượng dữ liệu:
   ```bash
   dbt test
   ```

## 📊 Dashboard (Looker Studio)
Kết nối Looker Studio với dataset `marts` trong BigQuery. Các chỉ số quan trọng:
- **Job Volume Trend**: Biểu thị nhu cầu thị trường theo thời gian.
- **Top Industries**: Các ngành nghề đang tuyển dụng mạnh nhất.
- **Apply Rate vs. Experience**: Phân biệt hiệu quả ứng tuyển giữa người mới (entry) và cấp cao (senior).

---
*Dự án được xây dựng dựa trên tập dữ liệu LinkedIn Job Postings 2023-2024.*

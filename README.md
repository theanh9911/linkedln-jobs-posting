# 💼 LinkedIn Job Market Analytics Pipeline

[![GCP](https://img.shields.io/badge/GCP-BigQuery-green.svg)](#)
[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](#)
[![dbt](https://img.shields.io/badge/dbt-v1.6+-orange.svg)](#)

Một hệ thống Data Pipeline hiện đại (Modern Data Stack) để thu thập, xử lý và phân tích hơn **120,000+** tin tuyển dụng từ LinkedIn (2023-2024).

---

## 📖 Thư Viện Tài Liệu (Project Wiki)

Hệ thống tài liệu được chuẩn hóa để cả **Người dùng** và **AI** đều có thể vận hành và phát triển dự án một cách nhanh nhất.

| Tài liệu | Nội dung chính | Trạng thái |
| :--- | :--- | :--- |
| **[🏛️ ARCHITECTURE.md](job-market-pipeline/docs/ARCHITECTURE.md)** | Luồng Cloud (ELT), Sơ đồ ERD, Các lớp Dữ liệu (Raw/Staging/Marts) | ✅ Đã hoàn thiện |
| **[🛠️ OPERATIONS.md](job-market-pipeline/docs/OPERATIONS.md)** | Cẩm nang lệnh `extract`, `load`, `bq`, `dbt` (Copy-Paste là chạy) | ✅ Đã hoàn thiện |
| **[📖 DATA_DICTIONARY.md](job-market-pipeline/docs/DATA_DICTIONARY.md)** | Danh sách 11 bảng, Khóa chính (PK), Khóa ngoại (FK) và Metrics | ✅ Đã hoàn thiện |

---

## 🚀 Bắt đầu nhanh (Quickstart)

Để vận hành dự án ngay lập tức, hãy đảm bảo bạn đã cấu hình file `.env` và file JSON Key trong thư mục `gcp_keys/`.

```bash
# 1. Trích xuất dữ liệu (CSV -> Parquet)
python job-market-pipeline/scripts/extract.py

# 2. Nạp dữ liệu lên Google Cloud (Bucket & BigQuery Raw)
python job-market-pipeline/scripts/load.py

# 3. Biến đổi dữ liệu bằng dbt (Staging & Marts)
cd job-market-pipeline/dbt && dbt run
```

---

## 📂 Cấu Trúc Thư Mục (Folder Structure)

```text
📁 linkedln-job-posting
├── 📁 data (Dữ liệu CSV gốc)
├── 📁 gcp_keys (Chứa file JSON xác thực)
└── 📁 job-market-pipeline
    ├── 📁 docs (Hệ thống tài liệu tri thức)
    ├── 📁 scripts (Mã nguồn Extract & Load)
    ├── 📁 dbt (Dự án biến đổi SQL)
    ├── .env (Cấu hình Project ID & Bucket Name)
    └── requirements.txt (Thư viện cần thiết)
```

---
> [!TIP]
> Nếu bạn muốn thêm bảng mới, chỉ cần copy file CSV vào thư mục `data/` và chạy lại các script trên. Hệ thống sẽ tự động quét và nạp bảng mới vào BigQuery!

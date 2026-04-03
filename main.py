import os
import subprocess
import logging
from datetime import datetime
from dotenv import load_dotenv

# Tải cấu hình từ .env
load_dotenv()

# Cấu hình Logging tổng thể cho Pipeline
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [MASTER] %(message)s',
    handlers=[
        logging.FileHandler("master_pipeline.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_command(command, cwd=None):
    """Thực thi một câu lệnh hệ thống và kiểm tra lỗi."""
    # Tạo bản sao môi trường và thêm UTF-8 enforcement
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    
    logger.info(f"Executing: {' '.join(command) if isinstance(command, list) else command}")
    try:
        result = subprocess.run(
            command,
            cwd=cwd,
            env=env,
            check=True,
            capture_output=True,
            text=True,
            shell=True
        )
        if result.stdout:
            logger.info(f"Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}")
        logger.error(f"Error output: {e.stderr}")
        return False

def main():
    start_time = datetime.now()
    
    # Tính toán đường dẫn tương đối cho GCP Keyfile để không bị hardcode ổ đĩa
    project_root = os.getcwd()
    keyfile_path = os.path.join(project_root, "gcp_keys", "application_default_credentials.json")
    os.environ["GCP_KEYFILE_PATH"] = keyfile_path
    
    logger.info("=== BẮT ĐẦU TOÀN BỘ PIPELINE PHÂN TÍCH LINKEDIN ===")

    # 1. Pipeline Extract (Python)
    logger.info(">>> Bước 1: Trích xuất & Làm sạch dữ liệu (CSV -> Parquet)")
    if not run_command(["uv", "run", "python", "job-market-pipeline/scripts/extract.py"]):
        logger.error("Dừng pipeline do lỗi tại bước Extract.")
        return

    # 2. Pipeline Load (Python)
    logger.info(">>> Bước 2: Nạp dữ liệu lên Google Cloud (GCS & BigQuery Raw)")
    if not run_command(["uv", "run", "python", "job-market-pipeline/scripts/load.py"]):
        logger.error("Dừng pipeline do lỗi tại bước Load.")
        return

    # 3. Pipeline dbt (SQL Transformation)
    logger.info(">>> Bước 3: Biến đổi dữ liệu & Kiểm soát chất lượng (dbt)")
    dbt_proj_dir = "job-market-pipeline/dbt"
    
    # Chạy dbt run từ Root với flags
    if not run_command(f"uv run dbt run --project-dir {dbt_proj_dir} --profiles-dir {dbt_proj_dir}"):
        logger.error("Dừng pipeline do lỗi tại bước dbt run.")
        return

    # Chạy dbt test từ Root với flags
    if not run_command(f"uv run dbt test --project-dir {dbt_proj_dir} --profiles-dir {dbt_proj_dir}"):
        logger.warning("Pipeline hoàn thành nhưng có lỗi trong dbt test. Hãy kiểm tra Data Quality.")
    else:
        logger.info("Toàn bộ Data Quality Tests đã PASS!")

    duration = datetime.now() - start_time
    logger.info(f"=== TOÀN BỘ PIPELINE HOÀN THÀNH TRONG {duration} ===")

if __name__ == "__main__":
    main()

import os
import subprocess
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv

# 1. Nạp cấu hình (Luôn ưu tiên file .env ở Root)
load_dotenv()

# 3. Cấu hình Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [MASTER] %(message)s',
    handlers=[
        logging.FileHandler("master_pipeline.log", encoding='utf-8', mode='a'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_command(command, cwd=None):
    """Thực thi câu lệnh và truyền toàn bộ môi trường hiện tại."""
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    
    # Tự động tìm Keyfile nếu có trong thư mục gcp_keys
    key_path = os.path.join(os.getcwd(), "gcp_keys", "application_default_credentials.json")
    if os.path.exists(key_path):
        env["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    logger.info(f"Executing: {command}")
    try:
        subprocess.run(command, cwd=cwd, env=env, check=True, shell=True)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Lỗi khi chạy lệnh: {command} (Exit code: {e.returncode})")
        return False

def main():
    start_time = datetime.now()
    logger.info("=" * 60)
    logger.info(">>> BẮT ĐẦU PIPELINE TỔNG THỂ (PIPELINE " + datetime.now().strftime("%Y-%m-%d %H:%M") + ")")
    logger.info("=" * 60)

    # Bước 1: Extract
    if not run_command("uv run python job-market-pipeline/scripts/extract.py"):
        return

    # Bước 2: Load
    if not run_command("uv run python job-market-pipeline/scripts/load.py"):
        return

    # Bước 3: dbt Build
    dbt_dir = "job-market-pipeline/dbt"
    if not run_command(f"uv run dbt build --project-dir {dbt_dir} --profiles-dir {dbt_dir}"):
        return

    duration = datetime.now() - start_time
    logger.info("=" * 60)
    logger.info(f">>> HOÀN THÀNH PIPELINE TRONG: {duration}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()


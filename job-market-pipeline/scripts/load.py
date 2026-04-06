import os
import glob
import logging
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

# Tải cấu hình từ .env nếu có
load_dotenv()

# Cấu hình Logging chuyên nghiệp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_load.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Cấu hình chứng chỉ (Sử dụng biến môi trường hoặc đường dẫn mặc định)
keyfile_path = os.getenv("GCP_KEYFILE_PATH")
# Lấy thư mục gốc của toàn bộ dự án (Lùi 3 cấp từ load.py: scripts -> job-market-pipeline -> ROOT)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if keyfile_path:
    # Nếu là đường dẫn tương đối, chuyển thành tuyệt đối dựa trên project root
    if not os.path.isabs(keyfile_path):
        keyfile_path = os.path.join(project_root, keyfile_path)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = keyfile_path
else:
    # Fallback mặc định
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
        project_root, "gcp_keys", "application_default_credentials.json"
    )


def ensure_bucket_exists(client, bucket_name, location="asia-southeast1"):
    """Kiểm tra và tạo GCS bucket nếu chưa tồn tại."""
    try:
        client.get_bucket(bucket_name)
        logger.info(f"Bucket {bucket_name} already exists.")
    except NotFound:
        logger.info(f"Bucket {bucket_name} not found. Creating it in {location}...")
        bucket = client.bucket(bucket_name)
        bucket.storage_class = "STANDARD"
        client.create_bucket(bucket, location=location)
        logger.info(f"Created bucket {bucket_name} in {location}.")
    except Exception as e:
        logger.error(f"Error checking/creating bucket {bucket_name}: {e}")

def ensure_dataset_exists(client, dataset_id, location="asia-southeast1"):
    """Kiểm tra và tạo dataset nếu chưa tồn tại."""
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_id} in {location}.")
    except Exception as e:
        logger.error(f"Error ensuring dataset {dataset_id}: {e}")


def upload_to_gcs(local_file, bucket_name, gcs_blob_name, project_id):
    """Tải file lên Google Cloud Storage."""
    if not os.path.exists(local_file):
        logger.error(f"File not found: {local_file}")
        return
    
    try:
        client = storage.Client(project=project_id)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_blob_name)
        logger.info(f"Uploading {local_file} to gs://{bucket_name}/{gcs_blob_name}...")
        blob.upload_from_filename(local_file)
        logger.info("Upload complete.")
    except Exception as e:
        logger.error(f"Failed to upload {local_file} to GCS: {e}")

def load_to_bigquery(gcs_uri, dataset_id, table_id, project_id, partition_field=None):
    """Nạp dữ liệu từ GCS vào BigQuery."""
    try:
        client = bigquery.Client(project=project_id)
        table_ref = f"{project_id}.{dataset_id}.{table_id}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        
        if partition_field:
            job_config.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_field
            )
        
        logger.info(f"Loading {gcs_uri} into {table_ref}...")
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        logger.info(f"Successfully loaded {load_job.output_rows} rows into {table_ref}.")
    except Exception as e:
        logger.error(f"Failed to load {gcs_uri} into BigQuery: {e}")

if __name__ == "__main__":
    PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    RAW_DATASET_ID = os.getenv("BQ_RAW_DATASET", "raw")
    REGION = os.getenv("GCP_REGION", "asia-southeast1")
    
    if not PROJECT_ID or not BUCKET_NAME:
        logger.error("Missing GOOGLE_CLOUD_PROJECT or GCS_BUCKET_NAME in .env file.")
        exit(1)

    # Base path is the project root (Three levels up from job-market-pipeline/scripts/load.py)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    processed_data_dir = os.path.join(project_root, "data", "processed")
    
    logger.info(f"Starting Load Pipeline (Root: {project_root})...")
    
    try:
        # 1. Đảm bảo kết nối Cloud
        bq_client = bigquery.Client(project=PROJECT_ID)
        gcs_client = storage.Client(project=PROJECT_ID)
        
        ensure_bucket_exists(gcs_client, BUCKET_NAME, location=REGION)
        ensure_dataset_exists(bq_client, RAW_DATASET_ID, location=REGION)

        
        # 2. Tìm file Parquet
        parquet_files = glob.glob(os.path.join(processed_data_dir, "*.parquet"))
        
        if not parquet_files:
            logger.error(f"No Parquet files found in {processed_data_dir}.")

            exit(1)
            
        for local_path in parquet_files:
            table_id = os.path.basename(local_path).replace(".parquet", "")
            gcs_blob = f"raw/{os.path.basename(local_path)}"
            gcs_uri = f"gs://{BUCKET_NAME}/{gcs_blob}"
            partition = "posted_date" if "postings" in table_id else None
            
            # 3. Upload & Load
            upload_to_gcs(local_path, BUCKET_NAME, gcs_blob, PROJECT_ID)
            load_to_bigquery(gcs_uri, RAW_DATASET_ID, table_id, PROJECT_ID, partition)
            
        logger.info("Load Pipeline Finished.")
        
    except Exception as e:
        logger.error(f"Critical error: {e}")



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

# Cấu hình chứng chỉ (Sử dụng đường dẫn tuyệt đối ổn định)
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    "gcp_keys", "application_default_credentials.json"
)

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
    PROJECT_ID = os.getenv("GCP_PROJECT_ID")
    BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
    RAW_DATASET_ID = os.getenv("BQ_RAW_DATASET", "raw")
    REGION = os.getenv("GCP_REGION", "asia-southeast1")
    
    if not PROJECT_ID or not BUCKET_NAME:
        logger.error("Missing GCP_PROJECT_ID or GCS_BUCKET_NAME in .env file.")
        exit(1)

    BASE_DIR = os.getcwd()
    PROCESSED_DATA_DIR = os.path.join(BASE_DIR, "data", "processed")
    
    start_time = datetime.now()
    logger.info("Starting Load Pipeline...")
    
    try:
        # 1. Đảm bảo kết nối BigQuery & Dataset tồn tại
        bq_client = bigquery.Client(project=PROJECT_ID)
        ensure_dataset_exists(bq_client, DATASET_ID)
        
        # 2. Tìm tất cả file Parquet đã xử lý
        parquet_files = glob.glob(os.path.join(PROCESSED_DATA_DIR, "*.parquet"))
        
        if not parquet_files:
            logger.error("No Parquet files found. Please run extract.py first.")
            exit(1)
            
        for local_path in parquet_files:
            filename = os.path.basename(local_path)
            # Tạo table_id từ filename (loại bỏ .parquet)
            table_id = filename.replace(".parquet", "")
            gcs_blob = f"raw/{filename}"
            gcs_uri = f"gs://{BUCKET_NAME}/{gcs_blob}"
            
            # Chỉ định partition cho bảng tin tuyển dụng nếu có cột posted_date
            partition = "posted_date" if "postings" in table_id else None
            
            # 3. Tải lên GCS
            upload_to_gcs(local_path, BUCKET_NAME, gcs_blob, PROJECT_ID)
            
            # 4. Nạp vào BigQuery
            load_to_bigquery(gcs_uri, DATASET_ID, table_id, PROJECT_ID, partition)
            
        duration = datetime.now() - start_time
        logger.info(f"Load Pipeline Finished in {duration}.")
        
    except Exception as e:
        logger.error(f"Critical error in Load Pipeline: {e}")

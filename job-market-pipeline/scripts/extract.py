import pandas as pd
import os
import glob
import logging
from datetime import datetime

# Cấu hình Logging chuyên nghiệp
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline_extract.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_dataframe(df, filename):
    """Làm sạch chung cho các dataframe."""
    logger.info(f"Cleaning {filename}...")
    
    try:
        # 1. Loại bỏ dòng trùng lặp dựa trên khóa chính nếu có
        if 'job_id' in df.columns and 'postings' in filename.lower():
            before = len(df)
            df = df.drop_duplicates(subset=['job_id']).dropna(subset=['job_id'])
            logger.info(f"Removed {before - len(df)} duplicate/null Job IDs.")
            
        elif 'company_id' in df.columns and 'companies' in filename.lower():
            before = len(df)
            df = df.drop_duplicates(subset=['company_id']).dropna(subset=['company_id'])
            logger.info(f"Removed {before - len(df)} duplicate/null Company IDs.")
        
        # 2. Xử lý thời gian đặc biệt cho postings
        if 'original_listed_time' in df.columns:
            df['posted_date'] = pd.to_datetime(df['original_listed_time'], unit='ms', errors='coerce').dt.date
            
        return df
    except Exception as e:
        logger.error(f"Error during cleaning {filename}: {e}")
        return df

def extract_all(raw_dir, processed_dir):
    """Tìm tất cả file CSV trong thư mục data/raw và chuyển đổi sang Parquet."""
    logger.info(f"Scanning for CSV files in {raw_dir}...")
    
    if not os.path.exists(raw_dir):
        logger.error(f"Raw data directory not found: {raw_dir}")
        return

    csv_files = glob.glob(os.path.join(raw_dir, "**", "*.csv"), recursive=True)
    logger.info(f"Found {len(csv_files)} CSV files.")
    
    for csv_path in csv_files:
        filename = os.path.basename(csv_path)
        relative_path = os.path.relpath(csv_path, raw_dir)
        output_filename = relative_path.replace(os.sep, "_").replace(".csv", ".parquet")
        output_path = os.path.join(processed_dir, output_filename)
        
        logger.info(f"Processing: {relative_path} -> {output_filename}")
        
        try:
            # Đọc CSV (Sử dụng low_memory=False cho file lớn)
            df = pd.read_csv(csv_path, low_memory=False)
            df = clean_dataframe(df, filename)
            
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df.to_parquet(output_path, engine='pyarrow', index=False)
            logger.info(f"Successfully saved to {output_path}")
            
        except Exception as e:
            logger.error(f"Failed to process {filename}: {e}")

if __name__ == "__main__":
    # Base path is the project root (where .env is)
    BASE_DIR = os.getcwd()
    RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
    PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
    
    start_time = datetime.now()
    logger.info("Starting Extract Pipeline...")
    
    extract_all(RAW_DIR, PROCESSED_DIR)
    
    duration = datetime.now() - start_time
    logger.info(f"Extract Pipeline Finished in {duration}.")

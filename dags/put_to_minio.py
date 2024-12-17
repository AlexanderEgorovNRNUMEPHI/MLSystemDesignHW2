import os
import pandas as pd
from io import BytesIO
from minio import Minio
from dotenv import load_dotenv
from loguru import logger
load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

def put_to_minio():
    client = Minio("minio:9000", access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
        logger.info(f"Bucket '{BUCKET_NAME}' created.")
    else:
        logger.info(f"Bucket '{BUCKET_NAME}' already exists.")
    
    for file in ["train.csv", "test.csv"]:
        file_path = f"./data/{file}"
        csv_data = pd.read_csv(file_path).to_csv(index=False).encode("utf-8")
        client.put_object(
            bucket_name=BUCKET_NAME,
            object_name=file,
            data=BytesIO(csv_data),
            length=len(csv_data),
            content_type="application/csv"
        )
        logger.info(f"Uploaded '{file}' to minio bucket.")
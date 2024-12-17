from fastapi import FastAPI
import json
from prometheus_fastapi_instrumentator import Instrumentator
from prometheus_client import Counter, Histogram
from minio import Minio
import os
import io
import pandas as pd
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from loguru import logger
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
Instrumentator().instrument(app).expose(app)


load_dotenv()

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("BUCKET_NAME")

logger.info(BUCKET_NAME)
# # Define a histogram metric
api_request_counter = Counter("api_request_counter", "Request processing time", ["method", "endpoint", "http_status"])
api_request_summary = Histogram("api_request_summary", "Request processing time", ["method", "endpoint"])
@app.get("/get_predictions/")
def get_predictions(user_id: int):
    
    api_request_counter.labels(method="GET", endpoint="/get_predictions", http_status=200).inc()
    api_request_summary.labels(method="GET", endpoint="/get_predictions").observe(0.1)
    

    
    client = Minio("minio:9000", access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)
    
    objects = client.list_objects(BUCKET_NAME, 'predictions', recursive=True)
    
    for obj in objects:
        name = obj.object_name
        logger.info(name)
        response = client.get_object(BUCKET_NAME, name)
        try:
            df = pd.read_csv(io.BytesIO(response.data))
        except pd.errors.EmptyDataError:
            continue
        df.columns = ["userId", "movieId", "rating","timestamp", "prediction"]
        df = df.sort_values(['userId','prediction'], ascending=[False, False])
        if df.userId.isin([user_id]).any():
            logger.info(df[df.userId == user_id]["movieId"])
            movie_id = df[df.userId == user_id]["movieId"].iloc[0]
            return f"prediction for user_id: {user_id} movie_id: {movie_id}"
    
    return f"There is no prediction for user_id: {user_id}" 
    
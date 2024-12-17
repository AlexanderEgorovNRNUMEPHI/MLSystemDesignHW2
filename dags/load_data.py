import requests
import zipfile
from loguru import logger

def load_data():
    url = "https://files.grouplens.org/datasets/movielens/ml-latest.zip"
    response = requests.get(url, verify=False)
    path = "ml-latest.zip"
    
    with open(path, "wb") as file:
        file.write(response.content)
    with zipfile.ZipFile(path, 'r') as zip_file:
        zip_file.extractall("data")
    logger.info("Data successfully downloaded and extracted.")
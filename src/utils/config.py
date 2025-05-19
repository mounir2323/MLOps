import os
from dotenv import load_dotenv

load_dotenv()

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001")
DATA_PATH = os.getenv("DATA_PATH", "../data/spotify_data.csv")
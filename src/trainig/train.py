import mlflow
import pandas as pd
from sklearn.model_selection import train_test_split
from .models.pipelines import *
from .utils.config import MLFLOW_TRACKING_URI

def main():
    # Chargement des données
    df = pd.read_csv("../data/spotify_data.csv")
    
    # Préparation des données
    X_train, X_val, y_train, y_val = train_test_split(
        df.drop(columns=['is_popular']), 
        df['is_popular'],
        test_size=0.2,
        random_state=42
    )
    
    # Configuration MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("Spotify-Classification")
    
    # Entraînement des modèles
    train_logistic_regression(X_train, y_train, X_val, y_val)
    train_catboost(X_train, y_train, X_val, y_val)

def train_logistic_regression(X_train, y_train, X_val, y_val):
    with mlflow.start_run(run_name="Logistic Regression"):
        pipeline = create_logistic_regression_pipeline()
        pipeline.fit(X_train, y_train)
        # Logging et évaluation...

def train_catboost(X_train, y_train, X_val, y_val):
    with mlflow.start_run(run_name="CatBoost"):
        pipeline = create_catboost_pipeline(get_class_weights(y_train))
        pipeline.fit(X_train, y_train)
        # Logging et évaluation...
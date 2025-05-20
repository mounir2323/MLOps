# ml_pipeline/assets/eval.py
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import matplotlib.pyplot as plt
import dagster as dg
import seaborn as sns
import mlflow
import pandas as pd
from xgboost import XGBClassifier

@dg.asset
def model_accuracy(split_data: dict, trained_model: XGBClassifier):
    # Configuration MLflow
    # mlflow.set_tracking_uri("http://localhost:5003")
    mlflow.set_experiment("Spotify Popularity Prediction")
    mlflow.sklearn.autolog()
    
    with mlflow.start_run(run_name="Evaluation Run"):
        X_test = split_data["X_test"]
        y_test = split_data["y_test"]
        
        # Prédictions
        y_pred = trained_model.predict(X_test)
        y_proba = trained_model.predict_proba(X_test)[:, 1]

        # Calcul des métriques
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        # Log des métriques
        metrics = {
            "ACC": accuracy,
            "PRC": precision,
            "REC": recall,
            "F1": f1,
        }
        mlflow.log_metrics(metrics)

        # Matrice de confusion
        plt.figure(figsize=(8, 6))
        cm = confusion_matrix(y_test, y_pred, normalize='true')
        sns.heatmap(cm, annot=True, fmt=".2f", cmap="Blues")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        plt.title("Confusion Matrix")
        plt.savefig("confusion_matrix.png")  # Sauvegarde avant log
        mlflow.log_artifact("confusion_matrix.png")
        plt.close()

        # Exemple de données (X_test au lieu de dt_test)
        X_test.head(10).to_csv("spotify_data_sample.csv")
        mlflow.log_artifact("spotify_data_sample.csv")

        # Log du modèle XGBoost
        mlflow.xgboost.log_model(trained_model, "model")
    
    return accuracy
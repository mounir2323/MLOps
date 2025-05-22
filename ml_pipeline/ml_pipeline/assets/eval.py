# ml_pipeline/assets/eval.py
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import matplotlib.pyplot as plt
import dagster as dg
import mlflow as mlflow_module  # Import avec alias pour éviter la collision
import seaborn as sns
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from ml_pipeline.resources.mlflow import MLflowTracking

@dg.asset
def model_accuracy(context: dg.AssetExecutionContext, split_data: dict, trained_model: XGBClassifier, mlflow: MLflowTracking):
    with mlflow.start_run(run_name="Evaluation Run") as run:
        X_test = split_data["X_test"]
        y_test = split_data["y_test"]

        # Prédictions pour la signature du modèle
        y_pred = trained_model.predict(X_test)
        
        # Calcul des métriques manuelles
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)

        # Log des métriques avec MLflow
        metrics = {
            "ACC": accuracy,
            "PRC": precision,
            "REC": recall,
            "F1": f1,
        }
        mlflow_module.log_metrics(metrics)

        # Matrice de confusion
        plt.figure(figsize=(8, 6))
        cm = confusion_matrix(y_test, y_pred, normalize='true')
        sns.heatmap(cm, annot=True, fmt=".2f", cmap="Blues")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        plt.title("Confusion Matrix")
        plt.savefig("confusion_matrix.png")
        mlflow_module.log_artifact("confusion_matrix.png")
        plt.close()

        # Exemple de données
        X_test.head(10).to_csv("spotify_data_sample.csv")
        mlflow_module.log_artifact("spotify_data_sample.csv")

        # Ajouter des métadonnées à l'asset
        context.add_output_metadata({
            "metrics": metrics
        })

    return accuracy
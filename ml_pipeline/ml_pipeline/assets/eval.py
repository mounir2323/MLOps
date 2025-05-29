# ml_pipeline/assets/eval.py
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, confusion_matrix
import matplotlib.pyplot as plt
import dagster as dg
import mlflow
import seaborn as sns
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from ml_pipeline.resources.mlflow import MLflowTracking



@dg.asset(io_manager_key="io_manager", required_resource_keys={"mlflow"})
def eval_model(context: dg.AssetExecutionContext, trained_model):
    """Évalue le modèle avec MLflow autolog et quelques métriques custom."""
    
    # Récupérer les données du modèle entraîné
    model = trained_model['model']
    X_test = trained_model['X_test']
    y_test = trained_model['y_test']
    run_id = trained_model['mlflow_run_id']
    
    # Configurer MLflow avec la ressource
    mlflow_resource = context.resources.mlflow
    mlflow_resource.setup_context()  # Configure le tracking URI et l'expérience
    
    # Reprendre le même run MLflow que l'entraînement
    with mlflow.start_run(run_id=run_id):
        # Tags additionnels pour l'évaluation
        mlflow.set_tags({
            "pipeline.stage": "training+evaluation",  # Indique que ce run contient les deux phases
            "evaluation.completed": "true",
            "dagster.eval_asset_name": "eval_model"
        })
        
        # Prédictions
        y_pred = model.predict(X_test)
        
        # Calcul des métriques d'évaluation personnalisées
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred)
        recall = recall_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred)
        
        # Log des métriques d'évaluation (avec préfixe pour les distinguer de l'autolog)
        mlflow.log_metrics({
            "eval_accuracy": accuracy,
            "eval_precision": precision,
            "eval_recall": recall,
            "eval_f1_score": f1
        })
        
        context.log.info(f"📊 Accuracy: {accuracy:.4f}")
        context.log.info(f"📊 Precision: {precision:.4f}")
        context.log.info(f"📊 Recall: {recall:.4f}")
        context.log.info(f"📊 F1-Score: {f1:.4f}")
        
        # Matrice de confusion (artifact personnalisé)
        cm = confusion_matrix(y_test, y_pred, normalize='true')
        sns.heatmap(cm, annot=True, fmt=".2f", cmap="Blues")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        plt.title("Confusion Matrix (XGBoost)")
        plt.savefig("Matrix_plot.png", dpi=300, bbox_inches='tight')
        mlflow.log_artifact("Matrix_plot.png")
        plt.close()  # Fermer la figure pour éviter les fuites mémoire
        
        # Log des métriques de la matrice de confusion (optionnel)
        tn, fp, fn, tp = cm.ravel()
        mlflow.log_metric("eval_true_negatives", float(tn))
        mlflow.log_metric("eval_false_positives", float(fp))
        mlflow.log_metric("eval_false_negatives", float(fn))
        mlflow.log_metric("eval_true_positives", float(tp))
        
        evaluation_results = {
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1_score': f1,
            'confusion_matrix': cm.tolist(),
            'mlflow_run_id': run_id  # Même run_id que l'entraînement
        }
        
        context.add_output_metadata({
            "accuracy": accuracy,
            "precision": precision, 
            "recall": recall,
            "f1_score": f1,
            "test_samples": len(X_test),
            "mlflow_run_id": run_id,
            "mlflow_run_url": f"http://localhost:5003/#/experiments/0/runs/{run_id}"
        })
        
        context.log.info(f"✅ Évaluation terminée dans le même run MLflow: {run_id}")
        
        return evaluation_results
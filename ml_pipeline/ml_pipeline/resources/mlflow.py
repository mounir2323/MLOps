# ml_pipeline/resources/mlflow.py
from dagster import ConfigurableResource
import mlflow
from mlflow.models import infer_signature

class MLflowTracking(ConfigurableResource):
    tracking_uri: str
    experiment_name: str
    enable_autolog: bool = True
    log_input_examples: bool = True
    log_model_signatures: bool = True

    def setup_context(self):
        """Configure MLflow au démarrage"""
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)
        
        if self.enable_autolog:
            # Active l'autologging pour plusieurs frameworks
            mlflow.sklearn.autolog(
                log_input_examples=self.log_input_examples,
                log_model_signatures=self.log_model_signatures
            )
            # Ajout de l'autologging XGBoost qui est essentiel pour vos modèles
            mlflow.xgboost.autolog(
                log_input_examples=self.log_input_examples,
                log_model_signatures=self.log_model_signatures
            )
            # Autologging général qui peut capturer d'autres métriques
            mlflow.autolog()

    def start_run(self, run_name: str):
        """Démarre une session MLflow avec le nom spécifié"""
        self.setup_context()
        return mlflow.start_run(run_name=run_name)

    def log_param(self, key: str, value):
        """Log un paramètre unique"""
        mlflow.log_param(key, value)

    def log_metric(self, key: str, value):
        """Log une métrique unique"""
        mlflow.log_metric(key, value)

    def log_metrics(self, metrics: dict):
        """Log plusieurs métriques à partir d'un dictionnaire"""
        mlflow.log_metrics(metrics)

    def log_artifact(self, local_path: str):
        """Log un fichier artifact"""
        mlflow.log_artifact(local_path)
        
    def infer_signature(self, inputs, outputs):
        """Inférer la signature du modèle"""
        return infer_signature(inputs, outputs)
    
    def evaluate(self, model, data, targets=None, model_type="classifier", evaluators=None):
        """Évaluation du modèle avec MLflow Evaluate"""
        evaluators = evaluators or ["default"]
        return mlflow.evaluate(
            model=model,
            data=data,
            targets=targets,
            model_type=model_type,
            evaluators=evaluators
        )

    def __getattr__(self, name):
        """Délégation au module MLflow pour les méthodes non implémentées"""
        return getattr(mlflow, name)
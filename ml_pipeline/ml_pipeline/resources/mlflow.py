# ml_pipeline/resources/mlflow.py
from dagster import resource
import mlflow

@resource(config_schema={
    "tracking_uri": str,
    "experiment_name": str
})
def mlflow_resource(context):
    # Configuration globale de MLflow
    mlflow.set_tracking_uri(context.resource_config["tracking_uri"])
    mlflow.set_experiment(context.resource_config["experiment_name"])
    return mlflow  # Retourne le module MLflow directement
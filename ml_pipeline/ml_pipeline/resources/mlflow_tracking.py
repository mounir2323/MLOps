import mlflow
from dagster import ConfigurableResource
import mlflow.sklearn


class MLflowTrackingResource(ConfigurableResource):
    tracking_uri: str
    experiment_name: str

    def start_run(self, run_name: str = None):
        mlflow.set_tracking_uri(self.tracking_uri)
        mlflow.set_experiment(self.experiment_name)
        return mlflow.start_run(run_name=run_name)

    def log_param(self, key, value):
        mlflow.log_param(key, value)

    def log_metric(self, key, value):
        mlflow.log_metric(key, value)

    def log_artifact(self, path):
        mlflow.log_artifact(path)

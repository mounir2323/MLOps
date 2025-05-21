from dagster import Definitions
from ml_pipeline.resources.mlflow import mlflow_resource
from ml_pipeline.assets import (
    preprocessing, 
    splitting,
    training,
    eval
)

defs = Definitions(
    assets=[
        preprocessing.preprocessed_data,
        splitting.split_data,
        training.trained_model,
        eval.model_accuracy
    ],
    resources={
        "mlflow": mlflow_resource.configured({
            "tracking_uri": "http://localhost:5003",
            "experiment_name": "Spotify Popularity Prediction"
        })
    }
)
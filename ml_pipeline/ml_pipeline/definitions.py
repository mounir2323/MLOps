from dagster import Definitions
from ml_pipeline.resources.mlflow import MLflowTracking

# Imports directs des assets depuis leurs modules respectifs
from ml_pipeline.assets.preprocessing import raw_data, cleaned_data, preprocessed_data
from ml_pipeline.assets.splitting import split_data
from ml_pipeline.assets.training import trained_model
from ml_pipeline.assets.eval import model_accuracy

defs = Definitions(
    assets=[
        raw_data,
        cleaned_data,
        preprocessed_data,
        split_data,
        trained_model,
        model_accuracy
    ],

    resources={
        "mlflow": MLflowTracking(
            tracking_uri="http://localhost:5003",
            experiment_name="Spotify Popularity Prediction",
            enable_autolog=True,
            log_input_examples=True,
            log_model_signatures=True
        )
    }
)
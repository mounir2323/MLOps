from dagster import Definitions
from ml_pipeline.resources.mlflow import MLflowTracking
from ml_pipeline.resources.lakeFS import LakeFSResource

# Imports directs des assets depuis leurs modules respectifs
from ml_pipeline.assets.preprocessing import raw_data, cleaned_data, preprocessed_data
from ml_pipeline.assets.splitting import split_data
from ml_pipeline.assets.training import trained_model
from ml_pipeline.assets.eval import eval_model
from ml_pipeline.io_manger.lakefs_io_manager import lakefs_io_manager


defs = Definitions(
    assets=[
        raw_data,
        cleaned_data,
        preprocessed_data,
        split_data,
        trained_model,
        eval_model
    ],

    resources={
        "mlflow": MLflowTracking(
            tracking_uri="http://localhost:5003",
            experiment_name="Spotify Popularity Prediction",
            enable_autolog=True,
            log_input_examples=True,
            log_model_signatures=True
        ),
    # Dans definitions.template.py
        "lakefs": LakeFSResource(
            endpoint="http://localhost:8000",
            access_key="YOUR_ACCESS_KEY_HERE",  # ← Remplace par un placeholder
            secret_key="YOUR_SECRET_KEY_HERE",  # ← Remplace par un placeholder
            repository="mlops-spoty-data",
            branch="main"
        ),
         "io_manager": lakefs_io_manager
    }
)


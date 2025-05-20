from dagster import Definitions
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
    ]
)
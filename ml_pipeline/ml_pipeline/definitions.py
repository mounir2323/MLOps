from dagster import Definitions
from ml_pipeline.assets.preprocessing import preprocessed_data
from ml_pipeline.assets.training import trained_model
from ml_pipeline.assets.eval import model_accuracy

defs = Definitions(
    assets=[preprocessed_data, trained_model, model_accuracy],
)

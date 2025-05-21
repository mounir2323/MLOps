# ml_pipeline/assets/training.py
import mlflow.sklearn
from xgboost import XGBClassifier
import dagster as dg
from dagster import ResourceDefinition, ResourceParam
from ml_pipeline.resources.mlflow import mlflow_resource

@dg.asset
def trained_model(context: dg.AssetExecutionContext, split_data: dict, mlflow: ResourceParam):
    with mlflow.start_run(run_name="Training Run"):
        X_train = split_data["X_train"]
        y_train = split_data["y_train"]

        model = XGBClassifier(
            objective="binary:logistic",
            eval_metric="logloss",
            use_label_encoder=False,
            random_state=42
        )

        # Auto-logging déclenché par fit()
        model.fit(X_train, y_train)

        # Ajouter des metadata custom si besoin
        context.add_output_metadata({
            "features_used": list(X_train.columns)
        })

    return model
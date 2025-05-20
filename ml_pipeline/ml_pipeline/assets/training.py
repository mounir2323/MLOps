# ml_pipeline/assets/training.py
import mlflow.sklearn
from xgboost import XGBClassifier
import dagster as dg
import mlflow
import mlflow.xgboost

@dg.asset
def trained_model(context: dg.AssetExecutionContext, split_data: dict):
    # Configuration MLflow
    mlflow.set_experiment("Spotify Popularity Prediction")
    
    # Activer autolog
    
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
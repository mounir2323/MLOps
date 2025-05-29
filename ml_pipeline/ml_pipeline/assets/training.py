# # ml_pipeline/assets/training.py
# import mlflow as mlflow_module  # Importation du module mlflow avec un alias
# import mlflow.sklearn
# import mlflow.xgboost

# from xgboost import XGBClassifier
# import dagster as dg
# from ml_pipeline.resources.mlflow import MLflowTracking
# import pandas as pd

# @dg.asset
# def trained_model(context: dg.AssetExecutionContext, split_data: pd.DataFrame, mlflow: MLflowTracking):
#     # Solution 2 : split_data est un DataFrame avec une colonne 'split'
#     train = split_data[split_data["split"] == "train"]
#     X_train = train.drop(["popularity", "split"], axis=1)
#     y_train = train["popularity"]
    
#     # Configuration de MLflow avant de créer le modèle
#     mlflow.setup_context()
    
#     model = XGBClassifier(
#         objective="binary:logistic",  # Objectif plus approprié pour XGBoost (au lieu de class_weight)
#         eval_metric="logloss",
#         use_label_encoder=False,
#         random_state=42
#     )
    
#     context.log.info("Auto-logging is enabled for XGBoost.")
    
#     # Utilisation de la ressource mlflow pour démarrer un run
#     with mlflow.start_run(run_name="train_XGBC") as run:
#         # Log explicite des paramètres pour s'assurer qu'ils sont capturés
#         mlflow_module.log_param("model_type", "XGBClassifier")
#         mlflow_module.log_param("features_count", len(X_train.columns))
        
#         # Créer un ensemble de validation pour l'évaluation pendant l'entraînement
#         from sklearn.model_selection import train_test_split
#         X_train_split, X_val, y_train_split, y_val = train_test_split(
#             X_train, y_train, test_size=0.2, random_state=42
#         )
#         eval_set = [(X_val, y_val)]
        
#         # Entraînement avec verbose et eval_set pour capturer les métriques d'entraînement
#         model.fit(
#             X_train_split, 
#             y_train_split,
#             eval_set=eval_set,
#             verbose=True
#         )
        
#         # Log manuel de quelques métriques d'évaluation pour s'assurer qu'elles sont capturées
#         import numpy as np
#         val_pred = model.predict(X_val)
#         val_accuracy = np.mean(val_pred == y_val)
#         mlflow_module.log_metric("validation_accuracy", val_accuracy)

#     return model



import pandas as pd
import dagster as dg
from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
import mlflow
import mlflow.xgboost

@dg.asset(io_manager_key="io_manager", required_resource_keys={"mlflow"})
def trained_model(context: dg.AssetExecutionContext, split_data):
    """Entraîne un modèle XGBoost avec MLflow autolog."""
    
    # Récupérer la ressource MLflow et configurer le tracking
    mlflow_resource = context.resources.mlflow
    mlflow_resource.setup_context()
    
    # Démarrer un run MLflow avec la ressource configurée
    with mlflow_resource.start_run(run_name=f"spotify_pipeline_{context.run_id[:8]}") as run:
        # Activer l'autolog XGBoost (fait tout automatiquement)
        mlflow.xgboost.autolog(
            log_input_examples=True,
            log_model_signatures=True,
            log_models=True,
        )
        
        # Tags personnalisés seulement (pas loggés automatiquement)
        mlflow.set_tags({
            "pipeline.stage": "training",
            "model.type": "xgboost", 
            "data.version": "v1",
            "dagster.run_id": context.run_id,
            "dagster.asset_name": "trained_model"
        })
        
        # Extraire les données
        train_data = split_data[split_data["split"] == "train"]
        test_data = split_data[split_data["split"] == "test"]
        
        X_train = train_data.drop(["popularity", "split"], axis=1)
        y_train = train_data["popularity"]
        X_test = test_data.drop(["popularity", "split"], axis=1)
        y_test = test_data["popularity"]
        
        context.log.info(f"🏋️ Entraînement sur {len(X_train)} échantillons")
        context.log.info(f"🔍 Test sur {len(X_test)} échantillons")
        
        # Créer et entraîner le modèle (autolog capture tout automatiquement)
        model = XGBClassifier(
            objective='binary:logistic',
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            random_state=42,
            eval_metric='logloss'
        )
        
        # L'entraînement avec eval_set - autolog capture les métriques automatiquement
        model.fit(
            X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=False
        )
        
        context.log.info("✅ Modèle entraîné avec autolog MLflow activé")
        
        return {
            'model': model,
            'X_test': X_test,
            'y_test': y_test,
            'mlflow_run_id': run.info.run_id
        }
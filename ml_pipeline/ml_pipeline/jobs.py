from dagster import define_asset_job

# Job 1 : Pipeline de préprocessing des données
# Exécute : raw_data -> cleaned_data -> preprocessed_data
preprocessing_job = define_asset_job(
    name="preprocessing_job",
    description="Pipeline d'ingestion et de nettoyage des données Spotify",
    selection=[
        "raw_data",
        "cleaned_data", 
        "preprocessed_data"
    ]
)

# Job 2 : Pipeline d'entraînement et d'évaluation
# Exécute : split_data -> trained_model -> eval_model
training_job = define_asset_job(
    name="training_job",
    description="Pipeline de split, entraînement et évaluation du modèle",
    selection=[
        "split_data",
        "trained_model",
        "eval_model"
    ]
)
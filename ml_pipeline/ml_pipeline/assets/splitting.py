# ml_pipeline/assets/splitting.py
from sklearn.model_selection import train_test_split
import dagster as dg
import pandas as pd

@dg.asset(io_manager_key="io_manager")
def split_data(context: dg.AssetExecutionContext, preprocessed_data: pd.DataFrame):
    X = preprocessed_data.drop("popularity", axis=1)
    y = preprocessed_data["popularity"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    train = X_train.copy()
    train["popularity"] = y_train
    train["split"] = "train"

    test = X_test.copy()
    test["popularity"] = y_test
    test["split"] = "test"

    combined = pd.concat([train, test], axis=0)
    return combined  # <-- retourne un DataFrame unique
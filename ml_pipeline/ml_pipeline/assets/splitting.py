# ml_pipeline/assets/splitting.py
from sklearn.model_selection import train_test_split
import dagster as dg
import pandas as pd

@dg.asset
def split_data(context: dg.AssetExecutionContext, preprocessed_data: pd.DataFrame):
    X = preprocessed_data.drop("popularity", axis=1)
    y = preprocessed_data["popularity"]
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, 
        test_size=0.2, 
        random_state=42,
        stratify=y
    )
    
    context.add_output_metadata({
        "train_size": len(X_train),
        "test_size": len(X_test),
        "class_balance": dict(y.value_counts(normalize=True))
    })
    
    return {
        "X_train": X_train,
        "X_test": X_test,
        "y_train": y_train,
        "y_test": y_test
    }
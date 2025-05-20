# ml_pipeline/assets/evaluation.py
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split
import dagster as dg

@dg.asset
def model_accuracy(preprocessed_data, trained_model):
    X = preprocessed_data.drop("popularity", axis=1)
    y = preprocessed_data["popularity"]
    _, X_test, _, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    y_pred = trained_model.predict(X_test)
    acc = accuracy_score(y_test, y_pred)

    return acc
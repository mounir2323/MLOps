from sklearn.model_selection import train_test_split
from xgboost import XGBClassifier
import dagster as dg

@dg.asset
def trained_model(preprocessed_data):
    X = preprocessed_data.drop("popularity", axis=1)
    y = preprocessed_data["popularity"]

    X_train, _, y_train, _ = train_test_split(X, y, test_size=0.2, random_state=42)
    model =  model = XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        use_label_encoder=False,
        random_state=42
    )
    model.fit(X_train, y_train)

    return model  # le modèle entraîné
from sklearn.pipeline import Pipeline
from catboost import CatBoostClassifier
from sklearn.linear_model import LogisticRegression
from .preprocessing.transformers import SpotifyPreprocessor

def create_logistic_regression_pipeline():
    return Pipeline([
        ('preprocessing', SpotifyPreprocessor()),
        ('classifier', LogisticRegression(
            class_weight='balanced',
            max_iter=1000,
            random_state=42
        ))
    ])

def create_catboost_pipeline(class_weights):
    return Pipeline([
        ('preprocessing', SpotifyPreprocessor()),
        ('classifier', CatBoostClassifier(
            class_weights=class_weights,
            depth=6,
            learning_rate=0.1,
            iterations=200,
            verbose=0,
            random_state=42
        ))
    ])
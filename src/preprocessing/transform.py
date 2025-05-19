import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

class SpotifyPreprocessor(BaseEstimator, TransformerMixin):
    def fit(self, X, y=None):
        # Implementation identique à votre version
        return self
    
    def transform(self, X):
        # Implementation identique à votre version
        return processed_data
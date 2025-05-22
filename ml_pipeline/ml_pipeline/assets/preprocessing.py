# ml_pipeline/assets/preprocessing.py
import pandas as pd
import dagster as dg
from sklearn.preprocessing import LabelEncoder


@dg.asset
def raw_data(context: dg.AssetExecutionContext):
    """Charge les données brutes du CSV."""
    df = pd.read_csv("data/spotify_data.csv")
    
    context.add_output_metadata({
        "raw_rows": len(df),
        "raw_columns": list(df.columns),
    })
    
    return df


@dg.asset
def cleaned_data(context: dg.AssetExecutionContext, raw_data: pd.DataFrame):
    """Nettoie les données en supprimant les colonnes inutiles et en transformant la popularité."""
    df = raw_data.copy()
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop(['track_id'], axis=1)
    df['popularity'] = (df['popularity'] >= 50).astype(int)
    
    context.log.info(f"Données nettoyées: {df.shape[0]} lignes, {df.shape[1]} colonnes")
    
    return df


@dg.asset
def preprocessed_data(context: dg.AssetExecutionContext, cleaned_data: pd.DataFrame):
    """Effectue l'encodage des variables catégorielles."""
    df = cleaned_data.copy()
    
    # Trouve les colonnes non numériques
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    context.log.info(f"Colonnes catégorielles détectées : {categorical_cols}")

    # Encode-les avec Label Encoding
    encoder = LabelEncoder()
    for col in categorical_cols:
        df[col] = encoder.fit_transform(df[col].astype(str))

    context.add_output_metadata({
        "rows": len(df),
        "columns": list(df.columns),
        "categorical_columns_encoded": categorical_cols
    })

    return df
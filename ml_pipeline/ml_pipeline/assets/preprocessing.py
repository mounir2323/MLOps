import pandas as pd
import dagster as dg
from sklearn.preprocessing import LabelEncoder
import os

@dg.asset(io_manager_key="io_manager")
def raw_data(context: dg.AssetExecutionContext):
    """Charge les données brutes depuis un CSV local."""
    csv_path = "data/spotify_data.csv" if os.path.exists("data/spotify_data.csv") else "ml_pipeline/data/spotify_data.csv"
    df = pd.read_csv(csv_path)
    
    context.add_output_metadata({
        "raw_rows": len(df),
        "raw_columns": list(df.columns),
    })
    
    return df  # L'IO Manager s'occupe de sauvegarder dans LakeFS

@dg.asset(io_manager_key="io_manager")
def cleaned_data(context: dg.AssetExecutionContext, raw_data: pd.DataFrame):
    """Nettoie les données."""
    df = raw_data.copy()
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop(['track_id'], axis=1)
    df['popularity'] = (df['popularity'] >= 50).astype(int)
    
    context.log.info(f"Données nettoyées: {df.shape[0]} lignes, {df.shape[1]} colonnes")
    
    return df  # L'IO Manager sauvegarde automatiquement dans clean_data/

@dg.asset(io_manager_key="io_manager")
def preprocessed_data(context: dg.AssetExecutionContext, cleaned_data: pd.DataFrame):
    """Effectue l'encodage des variables catégorielles."""
    df = cleaned_data.copy()
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
    
    encoder = LabelEncoder()
    for col in categorical_cols:
        df[col] = encoder.fit_transform(df[col].astype(str))
    
    context.add_output_metadata({
        "rows": len(df),
        "columns": list(df.columns),
        "categorical_columns_encoded": categorical_cols
    })
    
    return df  # L'IO Manager sauvegarde automatiquement dans processed/
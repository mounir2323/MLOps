# ml_pipeline/assets/preprocessing.py
import pandas as pd
import dagster as dg
from sklearn.preprocessing import LabelEncoder
import lakefs_client
from lakefs_client.models import CommitCreation
import os


@dg.asset
def raw_data(context: dg.AssetExecutionContext, lakefs: dg.ResourceParam):
    """Charge les données brutes depuis lakeFS ou les y sauvegarde."""
    import io
    
    # Initialiser le dépôt lakeFS
    lakefs.setup()
    
    try:
        # Tentative de récupération depuis lakeFS
        context.log.info("Tentative de charger les données depuis lakeFS")
        client = lakefs.get_client()
        response = client.objects.get_object(
            repository=lakefs.repository,
            ref="main",
            path="raw/spotify_data.csv"
        )
        # L'objet response est déjà un flux binaire, pas besoin de BytesIO
        df = pd.read_csv(response)
        context.log.info(f"Données chargées depuis lakeFS: {len(df)} lignes")
    except lakefs_client.exceptions.NotFoundException:
        # Charger depuis la source locale
        context.log.info("Chargement depuis la source locale")
        csv_path = "data/spotify_data.csv" if os.path.exists("data/spotify_data.csv") else "ml_pipeline/data/spotify_data.csv"
        df = pd.read_csv(csv_path)
        
        # Sauvegarder dans lakeFS
        bytes_content = io.BytesIO(df.to_csv(index=False).encode('utf-8'))
        client = lakefs.get_client()
        
        try:
            # Upload sur la branche principale
            client.objects.upload_object(
                repository=lakefs.repository,
                branch="main",
                path="raw/spotify_data.csv",
                content=bytes_content
            )
            client.commits.commit(
                repository=lakefs.repository,
                branch="main",
                commit_creation=CommitCreation(message="Initial data import")
            )
            context.log.info("Données sauvegardées dans lakeFS")
        except Exception as e:
            context.log.error(f"Erreur lors de l'upload: {e}")

    context.add_output_metadata({
        "raw_rows": len(df),
        "raw_columns": list(df.columns),
    })
    
    return df


@dg.asset
def cleaned_data(context: dg.AssetExecutionContext, raw_data: pd.DataFrame, lakefs: dg.ResourceParam):
    """Nettoie les données en supprimant les colonnes inutiles et en transformant la popularité."""
    import io
    
    df = raw_data.copy()
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop(['track_id'], axis=1)
    df['popularity'] = (df['popularity'] >= 50).astype(int)
    
    context.log.info(f"Données nettoyées: {df.shape[0]} lignes, {df.shape[1]} colonnes")
    
    # Enregistrer les données nettoyées dans lakeFS
    try:
        bytes_content = io.BytesIO(df.to_csv(index=False).encode('utf-8'))
        client = lakefs.get_client()
        
        # Upload sur la branche principale
        client.objects.upload_object(
            repository=lakefs.repository,
            branch="main",
            path="processed/cleaned_data.csv",
            content=bytes_content
        )
        
        # Commit les changements
        client.commits.commit(
            repository=lakefs.repository,
            branch="main",
            commit_creation=CommitCreation(message="Save cleaned data")
        )
        
        context.log.info("Données nettoyées sauvegardées dans lakeFS")
    except Exception as e:
        context.log.error(f"Erreur lors de l'enregistrement des données nettoyées: {e}")
    
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
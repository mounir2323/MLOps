# ml_pipeline/assets/preprocessing.py
import pandas as pd
import dagster as dg

@dg.asset
def preprocessed_data(context: dg.AssetExecutionContext):
    df = pd.read_csv("data/spotify_data.csv")
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop(['track_id'], axis=1)
    df['popularity'] = (df['popularity'] >= 50).astype(int)

    # Trouve les colonnes non numériques
    categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

    context.log.info(f"Colonnes catégorielles détectées : {categorical_cols}")

    # Encode-les avec Label Encoding
    from sklearn.preprocessing import LabelEncoder
    encoder = LabelEncoder()
    for col in categorical_cols:
        df[col] = encoder.fit_transform(df[col].astype(str))

    context.add_output_metadata({
        "rows": len(df),
        "columns": list(df.columns),
    })

    return df
import pandas as pd
import dagster as dg
from sklearn.preprocessing import LabelEncoder
import os
import io

@dg.asset(io_manager_key="io_manager", required_resource_keys={"lakefs"})
def raw_data(context: dg.AssetExecutionContext):
    """Charge les nouvelles données depuis incoming/raw/ ou fichiers locaux."""
    
    config = getattr(context.op_execution_context, 'op_config', {})
    if not config:
        run_config = getattr(context.op_execution_context, 'run_config', {})
        config = run_config.get('ops', {}).get('raw_data', {}).get('config', {})

    if config and config.get("process_lakefs_data"):
        lakefs = context.resources.lakefs
        detected_files = config.get("detected_files", [])
        
        if not detected_files:
            objects_response = lakefs.list_objects(branch="incoming", prefix="raw/")
            detected_files = [obj.path for obj in objects_response.results if obj.path.endswith(('.csv', '.parquet', '.json'))]
        
        new_data_list = []
        for file_path in detected_files:
            try:
                file_response = lakefs.get_object(branch="incoming", path=file_path)
                file_content = file_response.read()
                
                if file_path.endswith('.csv'):
                    df = pd.read_csv(io.StringIO(file_content.decode('utf-8')))
                elif file_path.endswith('.parquet'):
                    df = pd.read_parquet(io.BytesIO(file_content))
                else:
                    continue
                new_data_list.append(df)
            except:
                continue
        
        if new_data_list:
            df = pd.concat(new_data_list, ignore_index=True)
        else:
            raise ValueError("Aucune nouvelle donnée LakeFS trouvée")
            
    elif config and config.get("archived_files"):
        new_data_list = []
        for archived_file in config.get("archived_files", []):
            if os.path.exists(archived_file):
                new_data_list.append(pd.read_csv(archived_file, encoding='utf-8'))
        
        if new_data_list:
            df = pd.concat(new_data_list, ignore_index=True)
        else:
            raise ValueError("Aucun fichier archivé trouvé")
    else:
        csv_path = "data/spotify_data.csv" if os.path.exists("data/spotify_data.csv") else "ml_pipeline/data/spotify_data.csv"
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Fichier de données introuvable: {csv_path}")
        df = pd.read_csv(csv_path, encoding='utf-8')
    
    context.add_output_metadata({
        "raw_rows": len(df),
        "raw_columns": list(df.columns),
        "data_source": "lakefs" if (config and config.get("process_lakefs_data")) else "local"
    })
    
    return df

@dg.asset(io_manager_key="io_manager")
def cleaned_data(context: dg.AssetExecutionContext, raw_data: pd.DataFrame):
    """Nettoie les données."""
    df = raw_data.copy()
    
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = df.drop(['track_id'], axis=1, errors='ignore')
    df['popularity'] = (df['popularity'] >= 50).astype(int)
    
    context.add_output_metadata({
        "cleaned_rows": len(df),
        "cleaned_columns": list(df.columns)
    })
    
    return df

@dg.asset(io_manager_key="io_manager", required_resource_keys={"lakefs"})
def preprocessed_data(context: dg.AssetExecutionContext, cleaned_data: pd.DataFrame):
    """Encode les nouvelles données et les fusionne avec les anciennes."""
    
    # Encoder les nouvelles données
    df_new = cleaned_data.copy()
    categorical_cols = df_new.select_dtypes(include=['object']).columns.tolist()
    
    encoder = LabelEncoder()
    for col in categorical_cols:
        df_new[col] = encoder.fit_transform(df_new[col].astype(str))
    
    # Charger les anciennes données depuis main/processed/
    existing_data = pd.DataFrame()
    try:
        lakefs = context.resources.lakefs
        existing_objects = lakefs.list_objects(branch="main", prefix="processed/")
        
        for obj in existing_objects.results:
            if obj.path.endswith(('.parquet', '.csv')) and 'preprocessed_data' in obj.path:
                try:
                    file_content = lakefs.get_object(branch="main", path=obj.path)
                    if obj.path.endswith('.parquet'):
                        existing_data = pd.read_parquet(io.BytesIO(file_content.read()))
                    else:
                        existing_data = pd.read_csv(io.StringIO(file_content.read().decode('utf-8')))
                    break
                except:
                    continue
    except:
        pass
    
    # Fusion des données
    if not existing_data.empty:
        # Aligner les colonnes
        if set(df_new.columns) != set(existing_data.columns):
            all_columns = list(set(df_new.columns) | set(existing_data.columns))
            for col in all_columns:
                if col not in df_new.columns:
                    df_new[col] = 0
                if col not in existing_data.columns:
                    existing_data[col] = 0
            df_new = df_new[all_columns]
            existing_data = existing_data[all_columns]
        
        df_combined = pd.concat([existing_data, df_new], ignore_index=True).drop_duplicates()
        context.log.info(f"Fusion: {len(existing_data)} anciennes + {len(df_new)} nouvelles = {len(df_combined)} lignes")
    else:
        df_combined = df_new
    
    context.add_output_metadata({
        "total_rows": len(df_combined),
        "new_rows_added": len(df_new),
        "existing_rows": len(existing_data),
        "columns": list(df_combined.columns)
    })
    
    context.log.info(f"Données finales prêtes: {len(df_combined)} lignes")
    
    return df_combined
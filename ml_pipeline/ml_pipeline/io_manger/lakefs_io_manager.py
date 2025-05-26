from dagster import IOManager, io_manager
import pandas as pd
import pickle
import os

class LakeFSIOManager(IOManager):
    def __init__(self, lakefs_resource):
        self.lakefs = lakefs_resource
        # Par défaut, on utilise la branche "main" si non spécifiée
        self.branch = getattr(lakefs_resource, "branch", "main")

    def _get_path(self, context):
        # Récupère le nom de l'asset
        asset_key = context.asset_key.path[-1]
        
        # Définit les chemins selon l'asset avec la branche incluse
        if asset_key == "raw_data":
            path = f"s3://{self.lakefs.repository}/{self.branch}/raw/spotify_data.parquet"
        elif asset_key == "cleaned_data":
            path = f"s3://{self.lakefs.repository}/{self.branch}/clean_data/cleaned_spotify_data.parquet"
        elif asset_key == "trained_model":
            # Pour les modèles, on utilise pickle
            path = f"s3://{self.lakefs.repository}/{self.branch}/models/{asset_key}.pkl"
        else:
            # Pour tous les autres assets (preprocessed_data, split_data, etc.)
            path = f"s3://{self.lakefs.repository}/{self.branch}/processed/{asset_key}.parquet"
        
        # AJOUT DES LOGS POUR DEBUGGER
        print(f"🔍 [DEBUG] Asset: {asset_key}")
        print(f"🔍 [DEBUG] Repository: {self.lakefs.repository}")
        print(f"🔍 [DEBUG] Branch: {self.branch}")
        print(f"🔍 [DEBUG] Path généré: {path}")
        print(f"🔍 [DEBUG] Endpoint: {self.lakefs.endpoint}")
        
        return path
    
    def handle_output(self, context, obj):
        # Sauvegarde selon le type d'objet
        path = self._get_path(context)
        context.log.info(f"Sauvegarde dans LakeFS: {path}")
        
        asset_key = context.asset_key.path[-1]
        
        if asset_key == "trained_model":
            # Pour les modèles, on utilise pickle en local puis on upload
            local_path = f"temp_{asset_key}.pkl"
            with open(local_path, 'wb') as f:
                pickle.dump(obj, f)
            
            # Upload vers LakeFS (simulation avec copie locale pour l'instant)
            # Dans un vrai setup LakeFS, tu utiliserais l'API LakeFS ici
            os.makedirs("models", exist_ok=True)
            import shutil
            shutil.copy(local_path, f"models/{asset_key}.pkl")
            os.remove(local_path)
            context.log.info(f"Modèle sauvegardé localement dans models/{asset_key}.pkl")
            
        elif asset_key == "eval_model" or isinstance(obj, (int, float, str, bool)):
            # Pour les valeurs scalaires (métriques), on utilise pickle
            local_path = f"temp_{asset_key}.pkl"
            with open(local_path, 'wb') as f:
                pickle.dump(obj, f)
            
            os.makedirs("metrics", exist_ok=True)
            import shutil
            shutil.copy(local_path, f"metrics/{asset_key}.pkl")
            os.remove(local_path)
            context.log.info(f"Métrique sauvegardée localement dans metrics/{asset_key}.pkl")
            
        elif isinstance(obj, dict):
            # Si l'objet est un dictionnaire, sauvegarde chaque élément séparément
            for key, value in obj.items():
                # Construit un chemin pour chaque élément du dictionnaire
                item_path = f"{path.rstrip('.parquet')}_{key}.parquet"
                context.log.info(f"Sauvegarde de {key} dans {item_path}")
                
                # Convertit les Series en DataFrame si nécessaire
                if isinstance(value, pd.Series):
                    value = value.to_frame()
                    
                value.to_parquet(
                    item_path,
                    index=False,
                    storage_options={
                        "key": self.lakefs.access_key,
                        "secret": self.lakefs.secret_key,
                        "client_kwargs": {"endpoint_url": self.lakefs.endpoint}
                    }
                )
        else:
            # Si c'est un DataFrame directement
            obj.to_parquet(
                path,
                index=False,
                storage_options={
                    "key": self.lakefs.access_key,
                    "secret": self.lakefs.secret_key,
                    "client_kwargs": {"endpoint_url": self.lakefs.endpoint}
                }
            )
    
    def load_input(self, context):
        path = self._get_path(context.upstream_output)
        context.log.info(f"Chargement depuis LakeFS: {path}")
        
        asset_key = context.upstream_output.asset_key.path[-1]
        
        if asset_key == "trained_model":
            # Pour les modèles, on charge depuis pickle
            local_path = f"models/{asset_key}.pkl"
            if os.path.exists(local_path):
                with open(local_path, 'rb') as f:
                    return pickle.load(f)
            else:
                raise FileNotFoundError(f"Modèle non trouvé: {local_path}")
        elif asset_key == "eval_model":
            # Pour les métriques, on charge depuis pickle
            local_path = f"metrics/{asset_key}.pkl"
            if os.path.exists(local_path):
                with open(local_path, 'rb') as f:
                    return pickle.load(f)
            else:
                raise FileNotFoundError(f"Métrique non trouvée: {local_path}")
        else:
            # Chargement standard d'un DataFrame pour tous les autres assets
            return pd.read_parquet(
                path,
                storage_options={
                    "key": self.lakefs.access_key,
                    "secret": self.lakefs.secret_key,
                    "client_kwargs": {"endpoint_url": self.lakefs.endpoint}
                }
            )
@io_manager(required_resource_keys={"lakefs"})
def lakefs_io_manager(init_context):
    return LakeFSIOManager(init_context.resources.lakefs)
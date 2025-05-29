from dagster import IOManager, io_manager
import pandas as pd
import pickle
import os
import time

class LakeFSIOManager(IOManager):
    def __init__(self, lakefs_resource):
        self.lakefs = lakefs_resource
        # Par défaut, on utilise la branche "main" pour la sauvegarde finale
        self.branch = getattr(lakefs_resource, "branch", "main")

    def _get_path(self, context):
        # Récupère le nom de l'asset
        asset_key = context.asset_key.path[-1]
        
        # ✅ APPROCHE SIMPLIFIÉE : Sauvegarde locale d'abord, puis sync LakeFS plus tard
        if asset_key == "raw_data":
            # Nouvelles données brutes - sauvegarde locale temporaire
            path = f"temp_data/raw_data.parquet"
        elif asset_key == "cleaned_data":
            # Données nettoyées - sauvegarde locale temporaire
            path = f"temp_data/cleaned_data.parquet"
        elif asset_key == "preprocessed_data":
            # ✅ DONNÉES FINALES - sauvegarde locale finale
            path = f"data/preprocessed_data.parquet"
        elif asset_key == "trained_model":
            # Modèles dans dossier local
            path = f"models/{asset_key}.pkl"
        elif asset_key == "eval_model":
            # Métriques dans dossier local
            path = f"metrics/{asset_key}.pkl"
        else:
            # Autres assets
            path = f"temp_data/{asset_key}.parquet"
        
        # Logs pour debugging
        context.log.info(f"🔍 Asset: {asset_key} → Chemin local: {path}")
        
        return path
    
    def handle_output(self, context, obj):
        # Sauvegarde selon le type d'objet - APPROCHE LOCALE SIMPLIFIÉE
        path = self._get_path(context)
        context.log.info(f"💾 Sauvegarde locale: {path}")
        
        # Créer les dossiers nécessaires
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        asset_key = context.asset_key.path[-1]
        
        if asset_key in ["trained_model", "eval_model"]:
            # Pour les modèles et métriques, on utilise pickle
            with open(path, 'wb') as f:
                pickle.dump(obj, f)
            context.log.info(f"✅ {asset_key} sauvegardé en pickle: {path}")
            
        elif isinstance(obj, dict):
            # Si l'objet est un dictionnaire, on le sauvegarde en pickle pour simplicité
            with open(path.replace('.parquet', '.pkl'), 'wb') as f:
                pickle.dump(obj, f)
            context.log.info(f"✅ Dictionnaire sauvegardé en pickle: {path}")
            
        else:
            # Pour les DataFrames, sauvegarde en parquet
            if isinstance(obj, pd.DataFrame):
                obj.to_parquet(path, index=False)
                context.log.info(f"✅ DataFrame sauvegardé: {path} ({len(obj)} lignes)")
            else:
                # Fallback pour autres types
                with open(path.replace('.parquet', '.pkl'), 'wb') as f:
                    pickle.dump(obj, f)
                context.log.info(f"✅ Objet sauvegardé en pickle: {path}")
    
    def load_input(self, context):
        path = self._get_path(context.upstream_output)
        context.log.info(f"📂 Chargement depuis: {path}")
        
        asset_key = context.upstream_output.asset_key.path[-1]
        
        # Vérifier que le fichier existe
        if not os.path.exists(path):
            # Essayer avec extension .pkl si .parquet n'existe pas
            pkl_path = path.replace('.parquet', '.pkl')
            if os.path.exists(pkl_path):
                path = pkl_path
            else:
                raise FileNotFoundError(f"Fichier non trouvé: {path} ni {pkl_path}")
        
        if asset_key in ["trained_model", "eval_model"] or path.endswith('.pkl'):
            # Charger depuis pickle
            with open(path, 'rb') as f:
                data = pickle.load(f)
            context.log.info(f"✅ Chargé depuis pickle: {path}")
            return data
        else:
            # Charger DataFrame depuis parquet
            data = pd.read_parquet(path)
            context.log.info(f"✅ DataFrame chargé: {path} ({len(data)} lignes)")
            return data

@io_manager(required_resource_keys={"lakefs"})
def lakefs_io_manager(init_context):
    return LakeFSIOManager(init_context.resources.lakefs)
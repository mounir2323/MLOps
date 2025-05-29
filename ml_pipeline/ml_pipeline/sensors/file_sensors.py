import time
from dagster import sensor, RunRequest, SkipReason, DagsterRunStatus, RunsFilter
from ml_pipeline.jobs import preprocessing_job, training_job

@sensor(job=preprocessing_job, required_resource_keys={"lakefs"})
def lakefs_incoming_sensor(context):
    """Sensor qui surveille le dossier raw/ de la branche incoming de LakeFS"""
    lakefs = context.resources.lakefs
    
    try:
        context.log.info("🔍 Vérification du dossier incoming/raw/ dans LakeFS...")
        
        objects_response = lakefs.list_objects(
            branch="incoming",
            prefix="raw/"
        )
        
        # Filtrer uniquement les fichiers de données
        new_files = []
        for obj in objects_response.results:
            if obj.path.endswith(('.parquet', '.csv', '.json')):
                new_files.append({
                    'path': obj.path,
                    'size': obj.size_bytes,
                    'checksum': obj.checksum
                })
        
        context.log.info(f"🔍 {len(new_files)} fichiers trouvés")
        
        if not new_files:
            return SkipReason("Aucun fichier trouvé dans incoming/raw/")
        
        # État simple basé uniquement sur les checksums
        current_state = str(sorted([f['checksum'] for f in new_files]))
        
        if context.cursor == current_state:
            return SkipReason(f"Aucun changement détecté ({len(new_files)} fichiers)")
        
        # CHANGEMENTS DÉTECTÉS !
        context.update_cursor(current_state)
        context.log.info(f"🌊 CHANGEMENTS DÉTECTÉS ! {len(new_files)} fichiers")
        
        # Configuration ultra-simplifiée
        run_config = {
            "ops": {
                "raw_data": {
                    "config": {
                        "process_lakefs_data": True,
                        "detected_files": [f['path'] for f in new_files]
                    }
                }
            }
        }
        
        run_key = f"lakefs_{int(time.time())}"
        
        context.log.info(f"🚀 LANCEMENT du preprocessing_job avec {len(new_files)} fichiers")
        context.log.info(f"🚀 Run key: {run_key}")
        
        return RunRequest(
            run_key=run_key,
            run_config=run_config
        )
        
    except Exception as e:
        context.log.error(f"❌ Erreur sensor: {e}")
        return SkipReason(f"Erreur: {e}")

# ✅ SENSOR DIRECT : preprocessing → training
@sensor(job=training_job)
def preprocessing_to_training_sensor(context):
    """Sensor qui lance automatiquement training_job après la réussite de preprocessing_job"""
    
    try:
        context.log.info("🔍 Recherche des preprocessing_job réussis récents...")
        
        # Chercher les runs de preprocessing réussis
        runs = context.instance.get_runs(
            filters=RunsFilter(
                job_name="preprocessing_job",
                statuses=[DagsterRunStatus.SUCCESS]
            ),
            limit=5
        )
        
        if not runs:
            return SkipReason("Aucun preprocessing_job réussi trouvé")
        
        # Prendre le plus récent
        latest_run = runs[0]
        current_time = time.time()
        
        # Vérifier que le run est récent (moins de 10 minutes)
        run_end_time = getattr(latest_run, 'end_time', None) or getattr(latest_run, 'update_time', None)
        
        if run_end_time:
            try:
                end_timestamp = run_end_time.timestamp() if hasattr(run_end_time, 'timestamp') else run_end_time
                time_diff = current_time - end_timestamp
                
                if time_diff > 600:  # Plus de 10 minutes
                    return SkipReason(f"Dernier preprocessing trop ancien: {time_diff/60:.1f} minutes")
                
            except Exception as e:
                context.log.warning(f"⚠️ Erreur timestamp: {e}")
        
        # Vérifier qu'on n'a pas déjà lancé le training pour ce preprocessing
        cursor_key = f"training_for_{latest_run.run_id}"
        
        if context.cursor == cursor_key:
            return SkipReason(f"Training déjà lancé pour preprocessing {latest_run.run_id}")
        
        # LANCER LE TRAINING !
        context.update_cursor(cursor_key)
        
        run_key = f"auto_training_{latest_run.run_id}_{int(time.time())}"
        
        context.log.info(f"✅ Preprocessing réussi détecté: {latest_run.run_id}")
        context.log.info(f"🚀 LANCEMENT AUTOMATIQUE du training_job")
        context.log.info(f"🚀 Run key: {run_key}")
        
        return RunRequest(
            run_key=run_key,
            run_config={}  # Configuration par défaut
        )
        
    except Exception as e:
        context.log.error(f"❌ Erreur dans preprocessing_to_training_sensor: {e}")
        return SkipReason(f"Erreur: {e}")
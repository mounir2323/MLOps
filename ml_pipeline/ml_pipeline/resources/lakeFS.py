# ml_pipeline/resources/lakeFS.py
from dagster import ConfigurableResource, Field, get_dagster_logger
import lakefs_client
from lakefs_client.client import LakeFSClient
from lakefs_client.models import CommitCreation, BranchCreation, RepositoryCreation, StorageConfig
import io
import os

class LakeFSResource(ConfigurableResource):
    endpoint: str
    access_key: str
    secret_key: str
    repository: str
    
    def setup(self):
        """Initialise le dépôt lakeFS s'il n'existe pas déjà"""
        logger = get_dagster_logger()
        client = self.get_client()
        
        # Vérifier si le dépôt existe déjà
        try:
            client.repositories.get_repository(repository=self.repository)
            logger.info(f"Le dépôt '{self.repository}' existe déjà.")
        except lakefs_client.exceptions.NotFoundException:
            # Créer le dépôt s'il n'existe pas
            logger.info(f"Le dépôt '{self.repository}' n'existe pas. Création en cours...")
            try:
                repo_creation = RepositoryCreation(
                    name=self.repository,
                    storage_namespace="local://./lakefs_data",
                    default_branch="main"
                )
                client.repositories.create_repository(repository_creation=repo_creation)
                logger.info(f"Dépôt '{self.repository}' créé avec succès avec la branche par défaut 'main'.")
            except Exception as e:
                logger.error(f"Erreur lors de la création du dépôt: {e}")
                raise
            
    def get_client(self) -> LakeFSClient:
        """Retourne un client LakeFS configuré."""
        configuration = lakefs_client.Configuration(
            host=self.endpoint,
            username=self.access_key,
            password=self.secret_key
        )
        return LakeFSClient(configuration)

    def repository_exists(self) -> bool:
        """Vérifie si le dépôt existe"""
        client = self.get_client()
        try:
            client.repositories.get_repository(repository=self.repository)
            return True
        except lakefs_client.exceptions.NotFoundException:
            return False

    def branch_exists(self, branch_name: str) -> bool:
        """Vérifie si une branche existe."""
        client = self.get_client()
        try:
            client.branches.get_branch(
                repository=self.repository,
                branch=branch_name
            )
            return True
        except lakefs_client.exceptions.NotFoundException:
            return False

    def ensure_branch_exists(self, branch_name: str, source_ref: str = "main"):
        """Crée une branche si elle n'existe pas déjà."""
        if not self.branch_exists(branch_name):
            logger = get_dagster_logger()
            logger.info(f"Création automatique de la branche '{branch_name}'")
            return self.create_branch(branch_name, source_ref)
        return None

    def list_objects(self, branch: str, prefix: str = ""):
        """Liste les objets dans une branche avec un préfixe optionnel."""
        client = self.get_client()
        return client.objects.list_objects(
            repository=self.repository,
            ref=branch,
            prefix=prefix
        )

    def merge_branch(self, source_branch: str, target_branch: str, message: str = "Auto merge"):
        """Merge une branche source vers une branche cible."""
        client = self.get_client()
        return client.refs.merge_into_branch(
            repository=self.repository,
            source_ref=source_branch,
            destination_branch=target_branch
        )

    def delete_objects(self, branch: str, paths: list):
        """Supprime des objets d'une branche."""
        client = self.get_client()
        for path in paths:
            client.objects.delete_object(
                repository=self.repository,
                branch=branch,
                path=path
            )

    def create_branch(self, branch_name: str, source_ref: str = "main"):
        """Crée une nouvelle branche basée sur une référence source."""
        client = self.get_client()
        return client.branches.create_branch(
            repository=self.repository,
            branch_creation=BranchCreation(name=branch_name, source=source_ref)
        )

    def commit_changes(self, branch: str, message: str, metadata: dict = None):
        """Commit les changements dans une branche."""
        client = self.get_client()
        commit = CommitCreation(message=message, metadata=metadata)
        return client.commits.commit(
            repository=self.repository,
            branch=branch,
            commit_creation=commit
        )

    def upload_object(self, branch: str, path: str, content):
        """Upload un objet (fichier) vers lakeFS."""
        client = self.get_client()
        return client.objects.upload_object(
            repository=self.repository,
            branch=branch,
            path=path,
            content=content
        )
        
    def upload_file(self, branch: str, path: str, local_file_path: str):
        """Charge un fichier local vers lakeFS."""
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Le fichier local '{local_file_path}' n'existe pas")
            
        with open(local_file_path, 'rb') as f:
            content = io.BytesIO(f.read())
            return self.upload_object(branch, path, content)

    def get_object(self, branch: str, path: str):
        """Récupère un objet depuis lakeFS."""
        client = self.get_client()
        return client.objects.get_object(
            repository=self.repository,
            ref=branch,
            path=path
        )
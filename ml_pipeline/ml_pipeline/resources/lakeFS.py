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
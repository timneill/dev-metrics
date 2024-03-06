from dependency_injector import containers, providers
from database_manager import DatabaseManager
from github_api import GitHubAPI

class Container(containers.DeclarativeContainer):

    config = providers.Configuration()

    database = providers.Singleton(
        DatabaseManager,
        db_path=config.database.path
    )

    github_api_client = providers.Singleton(
        GitHubAPI,
        token=config.github.token,
        org=config.github.org,
        base_url=config.github.base_url,
    )

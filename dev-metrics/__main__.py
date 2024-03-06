import os
from datetime import datetime
from database_manager import DatabaseManager
from github_api import GitHubAPI
from container import Container
from utilities import calculate_cycle_time
import argparse
from typing import List, Any
from dependency_injector.wiring import Provide, inject

# Config variables
ORG = os.getenv("GH_TARGET_ORG")
TOKEN = os.getenv('GH_TOKEN')

# ------------------------------------------------------------------------------------------------------------------------------------------------
def insert_prs_to_db(prs: List[Any], database: DatabaseManager, github: GitHubAPI) -> None:
    for pr in prs:
        # Check if repo information is available
        repo_full_name = pr['head']['repo']['full_name'] if pr['head']['repo'] is not None else "Unknown or deleted repository"
        
        # Extract the PR author's username
        pr_author = pr['user']['login'] if pr['user'] and 'login' in pr['user'] else "Unknown author"
        
        # Insert detailed PR data into the database, including the author's username
        database.insert_pr_data(repo_full_name=repo_full_name, pr=pr, pr_author=pr_author)
        
        if pr['state'] == 'closed' or pr.get('merged_at'):
            first_commit_date, last_commit_date = github.fetch_pr_commit_dates(pr['base']['repo']['full_name'], pr['number'])
            pr_merged_at = pr.get('merged_at')
            pr_created_at = pr['created_at']
            pr_status = 'merged' if pr.get('merged_at') else 'closed'
            first_commit_datetime = datetime.strptime(first_commit_date, "%Y-%m-%dT%H:%M:%SZ")
            pr_created_datetime = datetime.strptime(pr_created_at, "%Y-%m-%dT%H:%M:%SZ")

            cycle_time_data = {
                'pr_number': pr['number'],
                'repo_full_name': pr['base']['repo']['full_name'],
                'pr_created_at': pr_created_at,
                'pr_merged_at': pr_merged_at,
                'pr_status': pr_status,
                'first_commit_date': first_commit_date,
                'last_commit_date': last_commit_date,
                'cycle_time': calculate_cycle_time(first_commit_datetime, pr_created_datetime, pr_merged_at)
            }
            database.insert_cycle_time_metrics(pr_data=cycle_time_data)

# ------------------------------------------------------------------------------------------------------------------------------------------------
@inject
def main(database: DatabaseManager = Provide[Container.database], github: GitHubAPI = Provide[Container.github_api_client]) -> None: 
    database.create_tables()

    parser = argparse.ArgumentParser(description="GitHub Repository Management Script")
    parser.add_argument('--fetch-prs', action='store_true', help="Fetch and update pull request information")
    parser.add_argument('--fetch-branches', action='store_true', help="Fetch and update branch information")
    args = parser.parse_args()
    
    if args.fetch_branches or args.fetch_prs:
        repos = github.fetch_org_repos()
        all_open_prs = []
        
        for repo in repos:
            last_fetch_timestamp = database.get_last_fetch_timestamp(repo['full_name']) or "1970-01-01T00:00:00Z"
            
            if github.has_repo_activity_since_last_fetch(repo_full_name=repo['full_name'], last_fetch_timestamp=last_fetch_timestamp):
                if args.fetch_branches:
                    print(f"Fetching latest commit data for branches in {repo['full_name']}")
                    branch_info = github.fetch_branch_latest_commit(repo_full_name=repo['full_name'])
                    if branch_info:
                        database.insert_branches_to_db(branches=branch_info)

                if args.fetch_prs:
                    print(f"Fetching open PRs for {repo['full_name']}")
                    open_prs = github.fetch_pull_requests_for_repo(repo_full_name=repo['full_name'])
                    all_open_prs.extend(open_prs)

                # Update the last fetch timestamp to the current time
                database.update_last_fetch_timestamp(repo['full_name'], datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
            else:
                print(f"No activity since last fetch for {repo['full_name']}, skipping...")
        
        if all_open_prs:
            insert_prs_to_db(all_open_prs, database, github)
            print("Updated PRs data in the database.")
        else:
            print("No updates to PRs data needed.")

# ------------------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    container = Container()

    container.config.database.path.from_env("DB_PATH")
    
    container.config.github.token.from_env("GH_TOKEN")
    container.config.github.org.from_env("GH_ORG")
    container.config.github.base_url.from_env("GH_BASE_URL")

    container.wire(modules=[__name__])

    main()

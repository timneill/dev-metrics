import os
from datetime import datetime
from database_manager import DatabaseManager
from github_api import GitHubAPI
import argparse
from typing import List, Any

# Config variables
ORG = "productsupcom"
TOKEN = os.getenv('TOKEN')
DB_PATH = "./output/github_prs.db"

# ------------------------------------------------------------------------------------------------------------------------------------------------
def calculate_cycle_time(first_commit_datetime, pr_created_datetime, pr_merged_at=None):
    # Use the earlier of 'first_commit_datetime' and 'pr_created_datetime' as the start date
    start_date = min(first_commit_datetime, pr_created_datetime)

    if pr_merged_at:
        # If the PR is merged, calculate the difference between the merge date and the start date
        pr_merged_datetime = datetime.strptime(pr_merged_at, "%Y-%m-%dT%H:%M:%SZ")
        cycle_time_days = (pr_merged_datetime - start_date).days
    else:
        # If the PR is not merged (e.g., closed without merging), consider the current time or another logic
        now = datetime.utcnow()
        cycle_time_days = (now - start_date).days

    return cycle_time_days

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
def main(): 
    database = DatabaseManager(db_path=DB_PATH)
    database.create_tables()

    github = GitHubAPI(token=TOKEN, org=ORG)

    parser = argparse.ArgumentParser(description="GitHub Repository Management Script")
    parser.add_argument('--fetch-prs', action='store_true', help="Fetch and update pull request information")
    parser.add_argument('--fetch-branches', action='store_true', help="Fetch and update branch information")
    args = parser.parse_args()

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
    main()

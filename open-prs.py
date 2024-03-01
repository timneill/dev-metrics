import csv
import os
import requests
import sqlite3
from datetime import datetime
from urllib.parse import quote
from requests.exceptions import HTTPError
import argparse

# Config variables
ORG = "productsupcom"
API_URL_BASE = "https://api.github.com"
TOKEN = os.getenv('TOKEN')
DB_PATH = "./output/github_prs.db"
AUTH_HEADERS = {
    'Authorization': f'Bearer {TOKEN}',
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28'
}

# ------------------------------------------------------------------------------------------------------------------------------------------------
def create_database(db_name=DB_PATH):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS pr_data (
                        id INTEGER PRIMARY KEY,
                        repo_full_name TEXT,
                        pr_number INTEGER,
                        pr_title TEXT,
                        pr_state TEXT,
                        pr_created_at TEXT,
                        pr_updated_at TEXT,
                        pr_url TEXT,
                        pr_author TEXT)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS branch_data (
                        id INTEGER PRIMARY KEY,
                        repo_full_name TEXT,
                        branch_name TEXT,
                        latest_commit_date TEXT,
                        branch_url TEXT)''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS cycle_time_metrics (
                        pr_number INTEGER PRIMARY KEY,
                        repo_full_name TEXT NOT NULL,
                        pr_created_at TEXT NOT NULL,
                        pr_merged_at TEXT,
                        pr_status TEXT,
                        cycle_time INTEGER, -- Calculated as the difference between pr_merged_at and pr_created_at in hours or days
                        first_commit_date TEXT,
                        last_commit_date TEXT)''')
    
    conn.commit()
    conn.close()

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_pr_commit_dates(repo_full_name, pr_number, headers):
    commits_url = f"{API_URL_BASE}/repos/{repo_full_name}/pulls/{pr_number}/commits"
    commits = fetch_all_pages(commits_url, headers)
    commit_dates = [commit['commit']['committer']['date'] for commit in commits]
    first_commit_date = min(commit_dates)
    last_commit_date = max(commit_dates)

    return first_commit_date, last_commit_date

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
def insert_prs_to_db(prs, db_name=DB_PATH):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    for pr in prs:
        # Check if repo information is available
        repo_full_name = pr['head']['repo']['full_name'] if pr['head']['repo'] is not None else "Unknown or deleted repository"
        
        # Extract the PR author's username
        pr_author = pr['user']['login'] if pr['user'] and 'login' in pr['user'] else "Unknown author"
        
        # Insert detailed PR data into the database, including the author's username
        cursor.execute('''INSERT INTO pr_data (repo_full_name, pr_number, pr_title, pr_state, pr_created_at, pr_updated_at, pr_url, pr_author)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                       (repo_full_name, pr['number'], pr['title'], pr['state'], pr['created_at'], pr['updated_at'], pr['html_url'], pr_author))
        if pr['state'] == 'closed' or pr.get('merged_at'):
            first_commit_date, last_commit_date = fetch_pr_commit_dates(pr['base']['repo']['full_name'], pr['number'], AUTH_HEADERS)
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
            insert_or_update_cycle_time_metrics(DB_PATH, cycle_time_data)

    conn.commit()
    conn.close()

# ------------------------------------------------------------------------------------------------------------------------------------------------
def insert_or_update_cycle_time_metrics(db_name, pr_data):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    sql = '''
        INSERT INTO cycle_time_metrics (pr_number, repo_full_name, pr_created_at, pr_merged_at, pr_status, cycle_time, first_commit_date, last_commit_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(pr_number) DO UPDATE SET
            repo_full_name = excluded.repo_full_name,
            pr_created_at = excluded.pr_created_at,
            pr_merged_at = excluded.pr_merged_at,
            pr_status = excluded.pr_status,
            cycle_time = excluded.cycle_time,
            first_commit_date = excluded.first_commit_date,
            last_commit_date = excluded.last_commit_date
    '''

    values = (
        pr_data['pr_number'],
        pr_data['repo_full_name'],
        pr_data['pr_created_at'],
        pr_data['pr_merged_at'],
        pr_data['pr_status'],
        pr_data['cycle_time'],
        pr_data['first_commit_date'],
        pr_data['last_commit_date']
    )

    cursor.execute(sql, values)

    conn.commit()
    conn.close()


# ------------------------------------------------------------------------------------------------------------------------------------------------
def update_last_fetch_timestamp(repo_full_name, last_fetch_timestamp, db_name=DB_PATH):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS last_fetch (
                        repo_full_name TEXT PRIMARY KEY,
                        last_fetch_timestamp TEXT)''')
    cursor.execute('''INSERT INTO last_fetch (repo_full_name, last_fetch_timestamp)
                      VALUES (?, ?)
                      ON CONFLICT(repo_full_name) 
                      DO UPDATE SET last_fetch_timestamp = excluded.last_fetch_timestamp''', 
                   (repo_full_name, last_fetch_timestamp))
    conn.commit()
    conn.close()

# ------------------------------------------------------------------------------------------------------------------------------------------------
def get_last_fetch_timestamp(repo_full_name, db_name=DB_PATH):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Ensure the last_fetch table exists
    cursor.execute('''CREATE TABLE IF NOT EXISTS last_fetch (
                        repo_full_name TEXT PRIMARY KEY,
                        last_fetch_timestamp TEXT)''')
    
    # Query the last fetch timestamp for the given repository
    cursor.execute('''SELECT last_fetch_timestamp FROM last_fetch WHERE repo_full_name = ?''', (repo_full_name,))
    result = cursor.fetchone()
    
    conn.close()
    
    # Return the last fetch timestamp if it exists
    if result:
        return result[0]
    else:
        return None

# ------------------------------------------------------------------------------------------------------------------------------------------------
def make_github_api_request(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # This will help catch HTTP errors
        
        # Check rate limit information
        rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
        rate_limit_reset = int(response.headers.get('X-RateLimit-Reset', 0))
        
        if rate_limit_remaining == 0:
            # Calculate reset time
            from datetime import datetime
            reset_time = datetime.fromtimestamp(rate_limit_reset).strftime('%Y-%m-%d %H:%M:%S')
            print(f"Rate limit exceeded. Resets at {reset_time}.")
        else:
            print(f"Rate limit remaining: {rate_limit_remaining}")
        
        return response
    except HTTPError as http_err:
        if response.status_code == 403 and 'rate limit' in response.text.lower():
            print("Error: Rate limit exceeded.")
        else:
            print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"Other error occurred: {err}")

# ------------------------------------------------------------------------------------------------------------------------------------------------
def has_repo_activity_since_last_fetch(repo_full_name, last_fetch_timestamp, headers):
    encoded_repo_full_name = quote(repo_full_name)

    url = f"{API_URL_BASE}/repos/{encoded_repo_full_name}/events"
    response = make_github_api_request(url, headers=headers)
    events = response.json()
    for event in events:
        event_timestamp = datetime.strptime(event['created_at'], "%Y-%m-%dT%H:%M:%SZ")
        if event_timestamp > datetime.strptime(last_fetch_timestamp, "%Y-%m-%dT%H:%M:%SZ"):
            return True
    return False

# ------------------------------------------------------------------------------------------------------------------------------------------------
def insert_branches_to_db(branches, db_name=DB_PATH):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    for branch in branches:
        cursor.execute('''INSERT INTO branch_data (repo_full_name, branch_name, latest_commit_date, branch_url)
                          VALUES (?, ?, ?, ?)''', 
                       (branch[0], branch[1], branch[3], branch[4]))  # Adjust the indices if necessary
    
    conn.commit()
    conn.close()

# ------------------------------------------------------------------------------------------------------------------------------------------------
def get_api_url_for_org(endpoint):
    return f"{API_URL_BASE}/orgs/{ORG}/{endpoint}".rstrip("/")

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_all_pages(url, headers):
    all_data = []
    while url:
        response = make_github_api_request(url, headers=headers)
        
        all_data.extend(response.json())

        # Parsing the Link header for pagination
        link_header = response.headers.get('Link', None)
        next_page_url = None
        if link_header:
            links = link_header.split(',')
            for link in links:
                if 'rel="next"' in link:
                    next_page_url = link.split(';')[0].strip(' <>')
                    break
        url = next_page_url

    return all_data

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_branches_and_commit_dates(repo_full_name, headers):
    branches_url = f"{API_URL_BASE}/repos/{repo_full_name}/branches"
    branches = fetch_all_pages(branches_url, headers)
    
    branch_info_list = []
    
    for branch in branches:
        branch_name = branch['name']
        encoded_repo_full_name = quote(repo_full_name)
        encoded_branch_name = quote(branch_name)

        commits_url = f"{API_URL_BASE}/repos/{encoded_repo_full_name}/commits?sha={encoded_branch_name}&per_page=1"
        commits = fetch_all_pages(commits_url, headers)

        if commits:
            earliest_commit_date = commits[-1]['commit']['committer']['date']
            latest_commit_date = commits[0]['commit']['committer']['date']
            # Assuming branch URL needs to be constructed as it's not directly available in branch API response
            branch_url = f"{API_URL_BASE}/repos/{encoded_repo_full_name}/branches/{encoded_branch_name}"
            branch_info_list.append((repo_full_name, branch_name, earliest_commit_date, latest_commit_date, branch_url))
    
    return branch_info_list

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_org_repos():
    url = get_api_url_for_org(f"repos?type=all&per_page=100")
    return fetch_all_pages(url, AUTH_HEADERS)

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_pull_requests_for_repo(repo_full_name):
    prs = []
    encoded_repo_full_name = quote(repo_full_name)
    url = f"{API_URL_BASE}/repos/{encoded_repo_full_name}/pulls?state=open&per_page=100"
    prs.extend(fetch_all_pages(url, AUTH_HEADERS))
    return prs

# ------------------------------------------------------------------------------------------------------------------------------------------------
def write_repos_to_csv(repos, filename="./output/github_org_repos.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Name", "URL", "Visibility", "Created At", "Last Push", "Description"])
        for repo in repos:
            visibility = "Private" if repo["private"] else "Public"
            writer.writerow([repo["name"], repo["html_url"], visibility, repo["created_at"], repo["pushed_at"], repo["description"]])

# ------------------------------------------------------------------------------------------------------------------------------------------------
def calculate_age(created_at):
    created_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    now = datetime.utcnow()
    age = (now - created_date).days
    return age

# ------------------------------------------------------------------------------------------------------------------------------------------------
def write_prs_to_csv(prs, filename="./output/github_org_open_prs.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Repository Name", "PR Title", "PR URL", "Age (days)"])
        for pr in prs:
            if pr['head']['repo'] is not None:
                repo_full_name = pr['head']['repo']['full_name']
            else:
                repo_full_name = "Unknown or deleted repository"
            
            age = calculate_age(pr['created_at'])
            writer.writerow([repo_full_name, pr['title'], pr['html_url'], age])

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_branch_latest_commit(repo_full_name, headers):
    encoded_repo_full_name = quote(repo_full_name)
    
    branches_url = f"{API_URL_BASE}/repos/{encoded_repo_full_name}/branches"
    branches = fetch_all_pages(branches_url, headers)
    
    branch_info_list = []
    
    for branch in branches:
        branch_name = branch['name']
        encoded_branch_name = quote(branch_name)
        commits_url = f"{API_URL_BASE}/repos/{encoded_repo_full_name}/commits?sha={encoded_branch_name}&per_page=1"
        response = make_github_api_request(commits_url, headers=headers)
        commits = response.json()
        
        if commits:
            latest_commit_date = commits[0]['commit']['committer']['date']
            # Assuming the branch URL is formed by appending the branch name to the repo URL
            branch_url = f"https://github.com/{encoded_repo_full_name}/tree/{encoded_branch_name}"
            # Note: Earliest commit date is not fetched due to inefficiency
            branch_info_list.append((repo_full_name, branch_name, "Unknown", latest_commit_date, branch_url))
    
    return branch_info_list

# ------------------------------------------------------------------------------------------------------------------------------------------------
def main(): 
    create_database(db_name=DB_PATH)

    parser = argparse.ArgumentParser(description="GitHub Repository Management Script")
    parser.add_argument('--fetch-prs', action='store_true', help="Fetch and update pull request information")
    parser.add_argument('--fetch-branches', action='store_true', help="Fetch and update branch information")
    args = parser.parse_args()

    repos = fetch_org_repos()
    all_open_prs = []

    for repo in repos:
        last_fetch_timestamp = get_last_fetch_timestamp(repo['full_name']) or "1970-01-01T00:00:00Z"
        
        if has_repo_activity_since_last_fetch(repo['full_name'], last_fetch_timestamp, AUTH_HEADERS):
            if args.fetch_branches:
                print(f"Fetching latest commit data for branches in {repo['full_name']}")
                branch_info = fetch_branch_latest_commit(repo['full_name'], AUTH_HEADERS)
                if branch_info:
                    insert_branches_to_db(branch_info)

            if args.fetch_prs:
                print(f"Fetching open PRs for {repo['full_name']}")
                open_prs = fetch_pull_requests_for_repo(repo['full_name'])
                all_open_prs.extend(open_prs)

            # Update the last fetch timestamp to the current time
            update_last_fetch_timestamp(repo['full_name'], datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"))
        else:
            print(f"No activity since last fetch for {repo['full_name']}, skipping...")
    
    if all_open_prs:
        insert_prs_to_db(all_open_prs)
        print("Updated PRs data in the database.")
    else:
        print("No updates to PRs data needed.")

# ------------------------------------------------------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    main()

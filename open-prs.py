import csv
import os
import requests
import sqlite3
from datetime import datetime

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
    
    # Updated table schema with pr_author and without pr_body
    cursor.execute('''CREATE TABLE IF NOT EXISTS pr_data (
                        id INTEGER PRIMARY KEY,
                        repo_full_name TEXT,
                        pr_number INTEGER,
                        pr_title TEXT,
                        pr_state TEXT,
                        pr_created_at TEXT,
                        pr_updated_at TEXT,
                        pr_url TEXT,
                        pr_author TEXT)''')  # Added pr_author
    
    conn.commit()
    conn.close()

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
def has_repo_activity_since_last_fetch(repo_full_name, last_fetch_timestamp, headers):
    url = f"{API_URL_BASE}/repos/{repo_full_name}/events"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    events = response.json()
    for event in events:
        event_timestamp = datetime.strptime(event['created_at'], "%Y-%m-%dT%H:%M:%SZ")
        if event_timestamp > datetime.strptime(last_fetch_timestamp, "%Y-%m-%dT%H:%M:%SZ"):
            return True
    return False

# ------------------------------------------------------------------------------------------------------------------------------------------------
def get_api_url_for_org(endpoint):
    return f"{API_URL_BASE}/orgs/{ORG}/{endpoint}".rstrip("/")

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_all_pages(url, headers):
    repos = []
    while url:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        repos.extend(response.json())
        if 'next' in response.links:
            url = response.links['next']['url']
        else:
            break
    return repos

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_org_repos():
    url = get_api_url_for_org(f"repos?type=all&per_page=100")
    return fetch_all_pages(url, AUTH_HEADERS)

# ------------------------------------------------------------------------------------------------------------------------------------------------
def fetch_pull_requests_for_repo(repo_full_name):
    prs = []
    url = f"{API_URL_BASE}/repos/{repo_full_name}/pulls?state=open&per_page=100"
    prs.extend(fetch_all_pages(url, AUTH_HEADERS))
    return prs

# ------------------------------------------------------------------------------------------------------------------------------------------------
def write_repos_to_csv(repos, filename="./output/github_org_repos.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Name", "URL", "Visibility", "Created At", "Last Push", "Description"])
        for repo in repos:
            # Determine visibility
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
            # Check if pr['head']['repo'] is not None
            if pr['head']['repo'] is not None:
                repo_full_name = pr['head']['repo']['full_name']
            else:
                # Handle the case where the repo information is not available
                # For example, use a placeholder or skip the PR
                repo_full_name = "Unknown or deleted repository"
            
            age = calculate_age(pr['created_at'])
            writer.writerow([repo_full_name, pr['title'], pr['html_url'], age])

# ------------------------------------------------------------------------------------------------------------------------------------------------
def main():
    create_database(db_name=DB_PATH)
    
    repos = fetch_org_repos()
    all_open_prs = []

    for repo in repos:
        last_fetch_timestamp = get_last_fetch_timestamp(repo['full_name']) or "1970-01-01T00:00:00Z"
        
        if has_repo_activity_since_last_fetch(repo['full_name'], last_fetch_timestamp, AUTH_HEADERS):
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

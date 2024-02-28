import csv
import os
import requests
from datetime import datetime

# Config variables
ORG = "productsupcom"
API_URL_BASE = "https://api.github.com"
TOKEN = os.getenv('TOKEN')
AUTH_HEADERS = {
    'Authorization': f'Bearer {TOKEN}',
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28'
}

def get_api_url_for_org(endpoint):
    return f"{API_URL_BASE}/orgs/{ORG}/{endpoint}".rstrip("/")

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

def fetch_org_repos():
    url = get_api_url_for_org(f"repos?type=all&per_page=100")
    return fetch_all_pages(url, AUTH_HEADERS)

def fetch_pull_requests_for_repo(repo_full_name):
    prs = []
    url = f"{API_URL_BASE}/repos/{repo_full_name}/pulls?state=open&per_page=100"
    prs.extend(fetch_all_pages(url, AUTH_HEADERS))
    return prs

def write_repos_to_csv(repos, filename="./output/github_org_repos.csv"):
    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["Name", "URL", "Visibility", "Created At", "Last Push", "Description"])
        for repo in repos:
            # Determine visibility
            visibility = "Private" if repo["private"] else "Public"
            writer.writerow([repo["name"], repo["html_url"], visibility, repo["created_at"], repo["pushed_at"], repo["description"]])

def calculate_age(created_at):
    created_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    now = datetime.utcnow()
    age = (now - created_date).days
    return age

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

def main():
    repos = fetch_org_repos()
    all_open_prs = []

    for repo in repos:
        print(f"Fetching open PRs for {repo['full_name']}")
        open_prs = fetch_pull_requests_for_repo(repo['full_name'])
        all_open_prs.extend(open_prs)
        print(f"Found {len(open_prs)} open PRs in {repo['full_name']}")

    print(f"Total open PRs: {len(all_open_prs)}")
    if all_open_prs:
        write_prs_to_csv(all_open_prs)
        print(f"Open PRs data has been written to 'github_org_open_prs.csv'.")
    else:
        print("No open PRs to write to CSV.")

if __name__ == "__main__":
    main()

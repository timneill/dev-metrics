import requests
from datetime import datetime
from urllib.parse import quote
from requests.exceptions import HTTPError
from typing import List, Any

class GitHubAPI:
    def __init__(self, token: str, org: str, base_url: str = "https://api.github.com"):
        self.token = token
        self.org = org
        self.base_url = base_url
        
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Accept': 'application/vnd.github+json',
            'X-GitHub-Api-Version': '2022-11-28'
        }

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def make_request(self, url: str, params: List[str] | None = None) -> Any:
        try:
            response = requests.get(url, headers=self.headers, params=params)
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
    def get_api_url_for_org(self, endpoint: str) -> str:
        return f"{self.base_url}/orgs/{self.org}/{endpoint}".rstrip("/")
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def fetch_all_pages(self, url: str) -> List[Any]:
        all_data = []
        while url:
            response = self.make_request(url)
            
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
    def fetch_org_repos(self) -> List[Any]:
        url = self.get_api_url_for_org(f"repos?type=all&per_page=100")
        return self.fetch_all_pages(url)
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def has_repo_activity_since_last_fetch(self, repo_full_name: str, last_fetch_timestamp) -> bool:
        encoded_repo_full_name = quote(repo_full_name)

        url = f"{self.base_url}/repos/{encoded_repo_full_name}/events" # TODO Keep watch to see if we also need to paginate
        response = self.make_request(url)
        events = response.json()

        for event in events:
            event_timestamp = datetime.strptime(event['created_at'], "%Y-%m-%dT%H:%M:%SZ")
            if event_timestamp > datetime.strptime(last_fetch_timestamp, "%Y-%m-%dT%H:%M:%SZ"):
                return True
        return False

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def fetch_branch_latest_commit(self, repo_full_name: str):
        encoded_repo_full_name = quote(repo_full_name)
        
        branches_url = f"{self.base_url}/repos/{encoded_repo_full_name}/branches"
        branches = self.fetch_all_pages(branches_url)
        
        branch_info_list = []
        
        for branch in branches:
            branch_name = branch['name']
            encoded_branch_name = quote(branch_name)
            commits_url = f"{self.base_url}/repos/{encoded_repo_full_name}/commits?sha={encoded_branch_name}&per_page=1"
            response = self.make_request(commits_url)
            commits = response.json()
            
            if commits:
                latest_commit_date = commits[0]['commit']['committer']['date']
                # Assuming the branch URL is formed by appending the branch name to the repo URL
                branch_url = f"https://github.com/{encoded_repo_full_name}/tree/{encoded_branch_name}"
                # Note: Earliest commit date is not fetched due to inefficiency
                branch_info_list.append((repo_full_name, branch_name, "Unknown", latest_commit_date, branch_url))
        
        return branch_info_list
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def fetch_pull_requests_for_repo(self, repo_full_name: str) -> List[Any]:
        prs = []
        encoded_repo_full_name = quote(repo_full_name)
        url = f"{self.base_url}/repos/{encoded_repo_full_name}/pulls?state=open&per_page=100"
        prs.extend(self.fetch_all_pages(url))

        return prs
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def fetch_pr_commit_dates(self, repo_full_name: str, pr_number: str, headers: List[Any]):
        commits_url = f"{self.base_url}/repos/{repo_full_name}/pulls/{pr_number}/commits"
        commits = self.fetch_all_pages(commits_url, headers)
        commit_dates = [commit['commit']['committer']['date'] for commit in commits]
        first_commit_date = min(commit_dates)
        last_commit_date = max(commit_dates)

        return first_commit_date, last_commit_date
    
    # ------------------------------------------------------------------------------------------------------------------------------------------------

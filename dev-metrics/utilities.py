from datetime import datetime

# ------------------------------------------------------------------------------------------------------------------------------------------------
def calculate_age(created_at):
    created_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    now = datetime.utcnow()
    age = (now - created_date).days
    return age

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
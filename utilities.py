from datetime import datetime

def calculate_age(created_at):
    created_date = datetime.strptime(created_at, "%Y-%m-%dT%H:%M:%SZ")
    now = datetime.utcnow()
    age = (now - created_date).days
    return age
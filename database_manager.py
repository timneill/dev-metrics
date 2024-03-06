import sqlite3
from contextlib import contextmanager
from typing import List, Any

# TODO: Named parameters for all prepared statements in this class

class DatabaseManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.db_path)
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def create_tables(self):
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.executescript('''CREATE TABLE IF NOT EXISTS pr_data (
                                id INTEGER PRIMARY KEY,
                                repo_full_name TEXT,
                                pr_number INTEGER,
                                pr_title TEXT,
                                pr_state TEXT,
                                pr_created_at TEXT,
                                pr_updated_at TEXT,
                                pr_url TEXT,
                                pr_author TEXT)''')
            
            cursor.executescript('''CREATE TABLE IF NOT EXISTS branch_data (
                                id INTEGER PRIMARY KEY,
                                repo_full_name TEXT,
                                branch_name TEXT,
                                latest_commit_date TEXT,
                                branch_url TEXT)''')
            
            cursor.executescript('''CREATE TABLE IF NOT EXISTS cycle_time_metrics (
                                pr_number INTEGER PRIMARY KEY,
                                repo_full_name TEXT NOT NULL,
                                pr_created_at TEXT NOT NULL,
                                pr_merged_at TEXT,
                                pr_status TEXT,
                                cycle_time INTEGER, -- Calculated as the difference between pr_merged_at and pr_created_at in hours or days
                                first_commit_date TEXT,
                                last_commit_date TEXT)''')
            
            cursor.executescript('''CREATE TABLE IF NOT EXISTS last_fetch (
                                repo_full_name TEXT PRIMARY KEY,
                                last_fetch_timestamp TEXT)''')

            conn.commit()

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def insert_pr_data(self, repo_full_name: str, pr: List[Any], pr_author: str) -> None:
        with self.connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO pr_data (repo_full_name, pr_number, pr_title, pr_state, pr_created_at, pr_updated_at, pr_url, pr_author)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', 
                       (repo_full_name, pr['number'], pr['title'], pr['state'], pr['created_at'], pr['updated_at'], pr['html_url'], pr_author))
        
            conn.commit()

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def insert_branches_to_db(self, branches: List[Any]) -> None:
        with self.connection() as conn:
            cursor = conn.cursor()
            
            for branch in branches:
                cursor.execute('''INSERT INTO branch_data (repo_full_name, branch_name, latest_commit_date, branch_url)
                                VALUES (?, ?, ?, ?)''', 
                            (branch[0], branch[1], branch[3], branch[4]))
            
            conn.commit()

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def update_last_fetch_timestamp(self, repo_full_name: str, last_fetch_timestamp) -> None:
        with self.connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''INSERT INTO last_fetch (repo_full_name, last_fetch_timestamp)
                            VALUES (?, ?)
                            ON CONFLICT(repo_full_name) 
                            DO UPDATE SET last_fetch_timestamp = excluded.last_fetch_timestamp''', 
                        (repo_full_name, last_fetch_timestamp))
            conn.commit()

    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def get_last_fetch_timestamp(self, repo_full_name: str):
        with self.connection() as conn:
            cursor = conn.cursor()
            
            # Query the last fetch timestamp for the given repository
            cursor.execute('''SELECT last_fetch_timestamp FROM last_fetch WHERE repo_full_name = ?''', (repo_full_name,))
            result = cursor.fetchone()
            
            # Return the last fetch timestamp if it exists
            if result:
                return result[0]
            else:
                return None
            
    # ------------------------------------------------------------------------------------------------------------------------------------------------
    def insert_cycle_time_metrics(self, pr_data: List[Any]) -> None:
        with self.connection() as conn:
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
                    last_commit_date = excluded.last_commit_date'''
            
            cursor.execute(sql, pr_data)

    # ------------------------------------------------------------------------------------------------------------------------------------------------


"""
scripts/migrate.py

Database migration script for ScuibJobsAi.
Can be executed via:
- npm run migrate
- python scripts/migrate.py
- Render build/deploy command

If DATABASE_URL or SUPABASE_DB_URL is provided, executes scripts/supabase_init.sql
directly against PostgreSQL.
If only SUPABASE_URL and SUPABASE_KEY are provided, verifies schema readiness
via the Supabase client and provides diagnostic feedback.
"""

import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parent.parent
try:
    from dotenv import load_dotenv
    load_dotenv(ROOT_DIR / ".env")
except ImportError:
    pass


def run_sql_migration(db_url: str, sql_file: Path) -> bool:
    """Executes SQL script using psycopg2."""
    try:
        import psycopg2
    except ImportError:
        print("[migrate] psycopg2 not installed. Attempting installation...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
        import psycopg2

    print(f"[migrate] Connecting to database to apply {sql_file.name}...")
    with open(sql_file, "r", encoding="utf-8") as f:
        sql = f.read()

    try:
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
        print(f"[migrate] Successfully applied {sql_file.name}")
        return True
    except Exception as e:
        print(f"[migrate] Error executing SQL migration: {e}")
        return False


def verify_supabase_client(url: str, key: str) -> bool:
    """Verifies that raw_jobs and parsed_jobs tables exist via REST API."""
    try:
        from supabase import create_client
        client = create_client(url, key)

        raw_res = client.table("raw_jobs").select("id").limit(1).execute()
        parsed_res = client.table("parsed_jobs").select("id").limit(1).execute()

        print("[migrate] Supabase tables 'raw_jobs' and 'parsed_jobs' verified and active.")
        return True
    except Exception as e:
        err_msg = str(e)
        if "PGRST205" in err_msg or "Could not find the table" in err_msg:
            print("[migrate] WARNING: Supabase tables 'raw_jobs' / 'parsed_jobs' not found.")
            print("[migrate] To automatically run migrations during deploy, provide DATABASE_URL,")
            print("[migrate] or run scripts/supabase_init.sql once in the Supabase SQL Editor.")
        else:
            print(f"[migrate] Notice: Supabase connectivity check result: {e}")
        return False


def main():
    print("[migrate] Running ScuibJobsAi migration check...")
    sql_file = ROOT_DIR / "scripts" / "supabase_init.sql"
    if not sql_file.exists():
        print(f"[migrate] Error: SQL file not found at {sql_file}")
        sys.exit(1)

    db_url = os.getenv("DATABASE_URL") or os.getenv("SUPABASE_DB_URL")
    if db_url:
        success = run_sql_migration(db_url, sql_file)
        if success:
            sys.exit(0)
        else:
            print("[migrate] Direct SQL execution failed; continuing check with API client...")

    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    if supabase_url and supabase_key:
        verify_supabase_client(supabase_url, supabase_key)
    else:
        print("[migrate] Notice: No database credentials configured in current environment.")
        print("[migrate] InMemoryStore will be used unless SUPABASE_URL/DATABASE_URL is set.")

    print("[migrate] Migration step completed.")
    sys.exit(0)


if __name__ == "__main__":
    main()

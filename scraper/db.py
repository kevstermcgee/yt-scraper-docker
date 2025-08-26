import time
import psycopg
from psycopg import OperationalError, errors
import socket

# Update these as appropriate
DB_CONFIG = {
    "host": "db",
    "port": 5432,
    "dbname": "mydatabase",
    "user": "myuser",
    "password": "mypassword"
}


def check_db_host_reachable():
    """Check if the database host is reachable"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((DB_CONFIG["host"], DB_CONFIG["port"]))
        sock.close()
        return result == 0
    except Exception as e:
        print(f"Host check failed: {e}")
        return False


def wait_for_db(max_retries=30, delay=5):
    print(f"Waiting for database at {DB_CONFIG['host']}:{DB_CONFIG['port']}...")

    for attempt in range(max_retries):
        try:
            # First check if host is reachable
            if not check_db_host_reachable():
                print(f"Attempt {attempt + 1}/{max_retries}: Database host not reachable, retrying in {delay}s...")
                time.sleep(delay)
                continue

            # Try to connect
            with psycopg.connect(**DB_CONFIG) as conn:
                # Test the connection with a simple query
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                print("Database connection established and tested.")
                return  # exit function when successful

        except OperationalError as e:
            error_msg = str(e).lower()
            if "connection refused" in error_msg:
                print(
                    f"Attempt {attempt + 1}/{max_retries}: Database not accepting connections yet, retrying in {delay}s...")
            elif "does not exist" in error_msg:
                print(f"Attempt {attempt + 1}/{max_retries}: Database doesn't exist yet, retrying in {delay}s...")
            else:
                print(f"Attempt {attempt + 1}/{max_retries}: DB error ({e}), retrying in {delay}s...")
            time.sleep(delay)

        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries}: Unexpected error ({e}), retrying in {delay}s...")
            time.sleep(delay)

    raise Exception(f"Failed to connect to DB after {max_retries} attempts.")


def table_exists():
    """Check if the youtube_links table exists"""
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                               SELECT EXISTS (SELECT
                                              FROM information_schema.tables
                                              WHERE table_name = 'youtube_links');
                               """)
                return cursor.fetchone()[0]
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


def ensure_table_exists():
    print("Ensuring database connection...")
    wait_for_db()  # Ensure connection is ready before trying to create a table

    try:
        print("Creating table if it doesn't exist...")
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                               CREATE TABLE IF NOT EXISTS youtube_links
                               (
                                   yt_ids TEXT NOT NULL UNIQUE
                               )
                               ''')
            conn.commit()
        print("Table 'youtube_links' ensured to exist.")

        # Verify table was created
        if table_exists():
            print("Table verification successful.")
        else:
            print("WARNING: Table verification failed!")

    except Exception as e:
        print(f"Failed to ensure table exists: {e}")
        raise


def save_link(links):
    if not links:
        print("No links to save.")
        return

    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                # Ensure table exists before inserting
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS youtube_links (
                        yt_ids TEXT NOT NULL UNIQUE
                    )
                ''')

                # Insert each link, skip duplicates using ON CONFLICT
                for link in links:
                    try:
                        cursor.execute(
                            "INSERT INTO youtube_links (yt_ids) VALUES (%s) ON CONFLICT (yt_ids) DO NOTHING;",
                            (link,)
                        )
                    except Exception as e:
                        print(f"Failed to insert link '{link}': {e}")

            conn.commit()

    except Exception as e:
        print(f"Error saving links to database: {e}")
        raise

def count_links():
    """Return the total number of rows in the youtube_links table"""
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM youtube_links;")
                result = cursor.fetchone()
                return result[0] if result else 0
    except Exception as e:
        print(f"Error counting links: {e}")
        return 0


def grab_link():
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT yt_ids FROM youtube_links ORDER BY RANDOM() LIMIT 1;")
                result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error in grab_link: {e}")
        return None

def grab_links_batch(batch_size=5):
    """Return `batch_size` random youtube links from the database"""
    try:
        with psycopg.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT yt_ids FROM youtube_links ORDER BY RANDOM() LIMIT %s;",
                    (batch_size,)
                )
                rows = cursor.fetchall()  # fetch all rows, not just one
        # Flatten the results if yt_ids is a list/array, otherwise just return the values
        links = []
        for row in rows:
            if isinstance(row[0], list):  # if your column stores arrays
                links.extend(row[0])
            else:
                links.append(row[0])
        return links
    except Exception as e:
        print(f"Error in grab_links_batch: {e}")
        return []


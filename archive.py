import os
import psycopg2
import gspread
import google.oauth2.service_account
import json
import sys
from datetime import datetime, timezone

# --- CONFIGURATION ---
BATCH_SIZE = 10000  # Process 10,000 rows at a time
ARCHIVE_OLDER_THAN_DAYS = 90 # Archive data older than 90 days (3 months)

# --- LOAD SECRETS ---
try:
    DB_URL = os.environ["DB_CONNECTION_STRING"]
    SHEET_NAME = "Disney Data Archive"  # Must match the name of your Google Sheet
    GDRIVE_KEY_JSON = os.environ["GDRIVE_SERVICE_ACCOUNT_KEY"]
except KeyError as e:
    print(f"---CRITICAL ERROR: Environment variable {e} not set.---", file=sys.stderr)
    sys.exit(1)

def auth_google():
    """Authenticate with Google Sheets using the service account JSON."""
    try:
        creds_json = json.loads(GDRIVE_KEY_JSON)
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = google.oauth2.service_account.Credentials.from_service_account_info(creds_json, scopes=scopes)
        gc = gspread.authorize(creds)
        print("Google Sheets authentication successful.")
        return gc
    except Exception as e:
        print(f"Error authenticating with Google: {e}", file=sys.stderr)
        return None

def get_or_create_worksheet(sheet, title, headers):
    """Get a worksheet by title, or create it with headers if it doesn't exist."""
    try:
        worksheet = sheet.worksheet(title)
        print(f"Found existing worksheet: '{title}'")
    except gspread.exceptions.WorksheetNotFound:
        print(f"Creating new worksheet: '{title}'...")
        worksheet = sheet.add_worksheet(title=title, rows=100, cols=20)
        worksheet.append_row(headers)
    return worksheet

def archive_table(db_conn, g_sheet, table_name, date_column, primary_key):
    """
    Fetches, appends, and deletes data in batches for a given table.
    """
    print(f"\n--- Starting archive for table: {table_name} ---")
    total_archived = 0
    
    with db_conn.cursor() as cursor:
        # Get column headers
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        headers = [desc[0] for desc in cursor.description]
        
        # Open the correct worksheet
        worksheet = get_or_create_worksheet(g_sheet, table_name, headers)
        
        while True:
            # 1. Fetch a batch of old rows
            print(f"Fetching batch of {BATCH_SIZE} old rows from {table_name}...")
            cursor.execute(
                f"""
                SELECT * FROM {table_name}
                WHERE {date_column} < (NOW() - INTERVAL '{ARCHIVE_OLDER_THAN_DAYS} days')
                ORDER BY {primary_key}
                LIMIT {BATCH_SIZE}
                """
            )
            rows = cursor.fetchall()

            if not rows:
                print("No more old rows found. Archive for this table is complete.")
                break # Exit the while loop

            # Convert rows to a list of lists (required by gspread)
            # and get a list of the IDs we are about to delete
            rows_to_append = []
            ids_to_delete = []
            
            for row in rows:
                # Convert all columns to strings for Google Sheets
                rows_to_append.append([str(col) for col in row])
                # Find the primary key's value
                pk_index = headers.index(primary_key)
                ids_to_delete.append(row[pk_index])

            # 2. Append this batch to Google Sheets
            print(f"Appending {len(rows_to_append)} rows to Google Sheet...")
            try:
                worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                print("Google Sheet append successful.")
            except Exception as e:
                print(f"---CRITICAL ERROR: Failed to append to Google Sheet: {e} ---", file=sys.stderr)
                print("ABORTING: No data will be deleted from Supabase to prevent data loss.")
                return # Stop the whole function

            # 3. Delete ONLY the rows we just appended
            print(f"Deleting {len(ids_to_delete)} rows from Supabase...")
            # Use a tuple for the "IN" clause
            ids_tuple = tuple(ids_to_delete)
            
            # Need a (safe) way to handle a single-item tuple
            if len(ids_tuple) == 1:
                ids_tuple = f"({ids_tuple[0]})"
                
            cursor.execute(f"DELETE FROM {table_name} WHERE {primary_key} IN {ids_tuple}")
            
            # Commit the delete
            db_conn.commit()
            
            total_archived += len(rows)
            print(f"Successfully deleted batch. Total archived so far: {total_archived}")

    return total_archived

def main():
    print("Starting archive service...")
    g_client = auth_google()
    if not g_client:
        sys.exit(1)
        
    try:
        g_sheet = g_client.open(SHEET_NAME)
    except Exception as e:
        print(f"---CRITICAL ERROR: Could not open Google Sheet named '{SHEET_NAME}'.---", file=sys.stderr)
        print("Did you share the sheet with the service account email?", file=sys.stderr)
        sys.exit(1)
        
    try:
        with psycopg2.connect(DB_URL) as conn:
            print("Supabase connection successful.")
            
            # Archive both tables
            archive_table(conn, g_sheet, 'wait_times', 'timestamp', 'id')
            archive_table(conn, g_sheet, 'park_operating_data', 'data_date', 'id')
            
            print("\nArchive run complete.")
            
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

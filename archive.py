import os
import psycopg2
import gspread
import google.oauth2.service_account
import json
import sys
from datetime import datetime, timezone

# --- CONFIGURATION ---
BATCH_SIZE = 5000
ARCHIVE_OLDER_THAN_DAYS = 1 

# --- LOAD SECRETS ---
try:
    DB_URL = os.environ["DB_CONNECTION_STRING"]
    GDRIVE_KEY_JSON = os.environ["GDRIVE_SERVICE_ACCOUNT_KEY"]
    USER_EMAIL = os.environ["MY_PERSONAL_EMAIL"]
    FOLDER_ID = os.environ["GDRIVE_FOLDER_ID"]
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

def get_spreadsheet_for_year(gc, year):
    """
    Finds the archive sheet for a specific year.
    If it doesn't exist, it creates it INSIDE the specific folder ID.
    """
    sheet_name = f"Disney Archive - {year}"
    
    try:
        # Try to open existing sheet
        sh = gc.open(sheet_name)
        print(f"Opened existing sheet: '{sheet_name}'")
        return sh
    except gspread.exceptions.SpreadsheetNotFound:
        print(f"Sheet '{sheet_name}' not found. Creating new workbook in folder {FOLDER_ID}...")
        try:
            # NEW: Create inside the specific folder
            sh = gc.create(sheet_name, folder_id=FOLDER_ID)
            
            # We still share it with you so it appears in your "Shared with me" list too
            sh.share(USER_EMAIL, perm_type='user', role='writer')
            print(f"Successfully created '{sheet_name}' and shared with {USER_EMAIL}")
            return sh
        except Exception as e:
            print(f"CRITICAL ERROR creating new sheet: {e}", file=sys.stderr)
            raise e

def get_or_create_worksheet(sh, title, headers):
    """Get a tab (worksheet) by title, or create it with headers."""
    try:
        worksheet = sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Creating new tab '{title}' in workbook '{sh.title}'...")
        worksheet = sh.add_worksheet(title=title, rows=100, cols=20)
        worksheet.append_row(headers)
    return worksheet

def archive_table(db_conn, gc, table_name, date_column, primary_key):
    """Fetches, appends, and deletes data."""
    print(f"\n--- Starting archive for table: {table_name} ---")
    total_archived = 0
    
    with db_conn.cursor() as cursor:
        # Get column headers
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 0")
        headers = [desc[0] for desc in cursor.description]
        
        try:
            date_col_index = headers.index(date_column)
        except ValueError:
            print(f"Error: Date column '{date_column}' not found.")
            return 0

        while True:
            print(f"Fetching batch of {BATCH_SIZE} old rows...")
            cursor.execute(
                f"""
                SELECT * FROM {table_name}
                WHERE {date_column} < (NOW() - INTERVAL '{ARCHIVE_OLDER_THAN_DAYS} days')
                ORDER BY {date_column} ASC 
                LIMIT {BATCH_SIZE}
                """
            )
            rows = cursor.fetchall()

            if not rows:
                print("No more old rows found.")
                break

            first_row_date = rows[0][date_col_index]
            if isinstance(first_row_date, str):
                data_year = first_row_date[:4]
            else:
                data_year = first_row_date.year

            try:
                sh = get_spreadsheet_for_year(gc, data_year)
                worksheet = get_or_create_worksheet(sh, table_name, headers)
            except Exception as e:
                print(f"Skipping batch due to Google Sheet error: {e}")
                break

            rows_to_append = []
            ids_to_delete = []
            
            for row in rows:
                row_date = row[date_col_index]
                row_year = row_date.year if not isinstance(row_date, str) else int(row_date[:4])
                
                if row_year != int(data_year):
                    break
                    
                rows_to_append.append([str(col) for col in row])
                pk_index = headers.index(primary_key)
                ids_to_delete.append(row[pk_index])

            if rows_to_append:
                print(f"Appending {len(rows_to_append)} rows to '{sh.title}'...")
                try:
                    worksheet.append_rows(rows_to_append, value_input_option='USER_ENTERED')
                except Exception as e:
                    print(f"Failed to append to Google Sheet: {e}", file=sys.stderr)
                    return 

                print(f"Deleting {len(ids_to_delete)} rows from Supabase...")
                ids_tuple = tuple(ids_to_delete)
                if len(ids_tuple) == 1:
                    ids_tuple = f"({ids_tuple[0]})"
                    
                cursor.execute(f"DELETE FROM {table_name} WHERE {primary_key} IN {ids_tuple}")
                db_conn.commit()
                total_archived += len(rows_to_append)
                print(f"Batch complete. Total archived: {total_archived}")

    return total_archived

def main():
    print("Starting Yearly Archive Service...")
    g_client = auth_google()
    if not g_client:
        sys.exit(1)

    try:
        with psycopg2.connect(DB_URL) as conn:
            print("Supabase connection successful.")
            archive_table(conn, g_client, 'wait_times', 'timestamp', 'id')
            archive_table(conn, g_client, 'park_operating_data', 'data_date', 'id')
            print("\nArchive run complete.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

import os
import requests
import psycopg2
import sys

# --- 1. CONFIGURATION ---
# Get the database connection string from the GitHub Secret
DB_URL = os.environ.get("DB_CONNECTION_STRING")

# The free API endpoint for all Walt Disney World parks
API_ENDPOINT = "https://api.themeparks.wiki/v1/entity/waltdisneyworldresort/live"

def fetch_wait_times():
    """Fetches live wait time data from the API."""
    try:
        print("Fetching data from API...")
        response = requests.get(API_ENDPOINT)
        response.raise_for_status()  # Raises an error for bad responses
        print("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from API: {e}", file=sys.stderr)
        return None

def save_to_database(data, conn):
    """Saves the relevant ride data to the PostgreSQL database."""
    rides_processed = 0

    # The 'liveData' key contains a list of parks and attractions
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return

    try:
        with conn.cursor() as cursor:
            for entity in data['liveData']:
                # We only want to save entities that are RIDES
                if entity.get('entityType') == 'ATTRACTION':

                    # Get park name from parent (e.g., "Magic Kingdom")
                    # We search for the parent entity of type "THEME_PARK"
                    park_name = "Unknown"
                    parent_id = entity.get('parent')
                    if parent_id:
                        # Find the parent entity in the main list
                        parent = next((e for e in data['liveData'] if e['id'] == parent_id and e.get('entityType') == 'THEME_PARK'), None)
                        if parent:
                            park_name = parent.get('name', 'Unknown')

                    ride_name = entity.get('name')
                    status = entity.get('status', 'Unknown')

                    # 'queue' contains wait time info if it exists
                    wait_time = None
                    if 'queue' in entity and 'STANDBY' in entity['queue']:
                        wait_time = entity['queue']['STANDBY'].get('waitTime')

                    # Only save if we have a ride name
                    if ride_name:
                        print(f"Saving: {park_name} - {ride_name} - Status: {status} - Wait: {wait_time}")

                        # This is the SQL command to insert our data
                        cursor.execute(
                            """
                            INSERT INTO wait_times (park_name, ride_name, wait_time_minutes, status)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (park_name, ride_name, wait_time, status)
                        )
                        rides_processed += 1

        conn.commit() # Commit all changes to the database
        print(f"Successfully saved data for {rides_processed} rides.")

    except Exception as e:
        print(f"Error during database operation: {e}", file=sys.stderr)
        conn.rollback() # Rollback changes on error

def get_main_park_statuses(data):
    """Finds the 4 main theme parks and returns their current status."""

    # These names must match the API data exactly
    main_park_names = [
        "Magic Kingdom Park",
        "Epcot",  # The API data uses "Epcot", not "EPCOT"
        "Disney's Hollywood Studios",
        "Disney's Animal Kingdom Theme Park"
    ]

    park_statuses = {}

    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return None

    # Find the 4 main park entities
    for entity in data['liveData']:
        if entity.get('entityType') == 'THEME_PARK' and entity.get('name') in main_park_names:
            park_name = entity['name']
            status = entity.get('status', 'Unknown')
            park_statuses[park_name] = status
            print(f"Status check: {park_name} is {status}")

    return park_statuses

def main():
    if not DB_URL:
        print("Error: DB_CONNECTION_STRING secret is not set.", file=sys.stderr)
        sys.exit(1)

    api_data = fetch_wait_times()

    if api_data:

        park_statuses = get_main_park_statuses(api_data)

        if park_statuses:
            # Check if ALL main parks are closed
            all_closed = all(status == 'CLOSED' for status in park_statuses.values())

            # We want to make sure we found all 4 parks before trusting the "all_closed" check
            # This prevents a bad API response from shutting down the script
            found_all_parks = len(park_statuses) == 4

            if found_all_parks and all_closed:
                print("All 4 main parks are reporting 'CLOSED'. Exiting script.")
                sys.exit(0) # Exit successfully without error
            elif not found_all_parks:
                print("Warning: Did not find all 4 main parks in API response. Proceeding to save data just in case.")
            else:
                print("At least one main park is open. Proceeding to save data.")
        else:
            print("Could not determine park statuses. Proceeding to save data.")

        
        try:
            # Connect to the Supabase database
            with psycopg2.connect(DB_URL) as conn:
                print("Database connection successful.")
                save_to_database(api_data, conn)
        except psycopg2.OperationalError as e:
            print(f"Error connecting to database: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()

import os
import requests
import psycopg2
import sys

# --- 1. CONFIGURATION ---
DB_URL = os.environ.get("DB_CONNECTION_STRING")
API_ENDPOINT = "https://api.themeparks.wiki/v1/entity/waltdisneyworldresort/live"

def fetch_wait_times():
    """Fetches live wait time data from the API."""
    try:
        print("Fetching data from API...")
        response = requests.get(API_ENDPOINT)
        response.raise_for_status()
        print("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching from API: {e}", file=sys.stderr)
        return None

def get_main_park_statuses(data):
    """Finds the 4 main theme parks and returns their current status."""
    main_park_names = [
        "Magic Kingdom Park",
        "Epcot",
        "Disney's Hollywood Studios",
        "Disney's Animal Kingdom Theme Park"
    ]
    park_statuses = {}
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return None
    
    for entity in data['liveData']:
        if entity.get('entityType') == 'THEME_PARK' and entity.get('name') in main_park_names:
            park_name = entity['name']
            status = entity.get('status', 'Unknown')
            park_statuses[park_name] = status
            print(f"Status check: {park_name} is {status}")
    
    return park_statuses

# --- We no longer need the find_parent_park function ---

def save_to_database(data, conn):
    """Saves the relevant ride data to the PostgreSQL database."""
    rides_processed = 0
    
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return

    # --- Build a quick-lookup map of all entities ---
    # We still need this to map the parkId to its name
    try:
        entity_map = {entity['id']: entity for entity in data['liveData']}
    except KeyError as e:
        print(f"Error building entity map, an entity was missing an 'id': {e}", file=sys.stderr)
        return
    except TypeError as e:
        print(f"Error building entity map, 'liveData' was not a list: {e}", file=sys.stderr)
        return

    try:
        with conn.cursor() as cursor:
            for entity in data['liveData']:
                # We only want to save ATTRACTION entities
                if entity.get('entityType') == 'ATTRACTION':
                    
                    # --- NEW, SIMPLIFIED PARK NAME LOGIC ---
                    park_name = "Unknown" # Default
                    park_id = entity.get('parkId') # Get the direct parkId
                    
                    if park_id:
                        # Use the parkId to look up the park's entity in our map
                        park_entity = entity_map.get(park_id)
                        if park_entity:
                            # Get the name from that park's entity
                            park_name = park_entity.get('name', 'Unknown')
                        else:
                            print(f"Warning: Found parkId {park_id} but no matching entity in map.")
                    # --- END NEW LOGIC ---
                    
                    ride_name = entity.get('name')
                    status = entity.get('status', 'Unknown')
                    
                    # 'queue' contains wait time info if it exists
                    wait_time = None
                    if 'queue' in entity and 'STANDBY' in entity['queue']:
                        wait_time = entity['queue']['STANDBY'].get('waitTime')
                    
                    if ride_name:
                        cursor.execute(
                            """
                            INSERT INTO wait_times (park_name, ride_name, wait_time_minutes, status)
                            VALUES (%s, %s, %s, %s)
                            """,
                            (park_name, ride_name, wait_time, status)
                        )
                        rides_processed += 1
        
        conn.commit()
        print(f"Successfully saved data for {rides_processed} rides.")
    
    except Exception as e:
        print(f"Error during database operation: {e}", file=sys.stderr)
        conn.rollback()

def main():
    DB_URL = os.environ.get("DB_CONNECTION_STRING")
    
    if not DB_URL:
        print("---CRITICAL ERROR---", file=sys.stderr)
        print("The 'DB_CONNECTION_STRING' secret was not found.", file=sys.stderr)
        sys.exit(1)
    
    print("Successfully loaded DB_CONNECTION_STRING secret.")

    api_data = fetch_wait_times()
    
    if api_data:
        
        park_statuses = get_main_park_statuses(api_data)
        
        if park_statuses:
            all_closed = all(status == 'CLOSED' for status in park_statuses.values())
            found_all_parks = len(park_statuses) == 4

            if found_all_parks and all_closed:
                print("All 4 main parks are reporting 'CLOSED'. Exiting script.")
                sys.exit(0)
            elif not found_all_parks:
                print("Warning: Did not find all 4 main parks in API response. Proceeding to save data just in case.")
            else:
                print("At least one main park is open. Proceeding to save data.")
        else:
            print("Could not determine park statuses. Proceeding to save data.")

        try:
            with psycopg2.connect(DB_URL) as conn:
                print("Database connection successful.")
                save_to_database(api_data, conn)
        except psycopg2.OperationalError as e:
            print(f"Error connecting to database: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()

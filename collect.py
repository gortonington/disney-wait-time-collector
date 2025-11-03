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

def get_main_park_data(data):
    """
    Finds the 4 main theme parks and returns their full operating data.
    Returns a list of dictionaries.
    """
    main_park_names = [
        "Magic Kingdom Park",
        "Epcot",
        "Disney's Hollywood Studios",
        "Disney's Animal Kingdom Theme Park"
    ]
    
    park_data_list = []
    
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return [] # Return an empty list

    # --- NEW: Check for both types ---
    park_entity_types = ["THEME_PARK", "PARK"]

    for entity in data['liveData']:
        # --- MODIFIED: Check if entityType is in our list ---
        if entity.get('entityType') in park_entity_types and entity.get('name') in main_park_names:
            name = entity['name']
            status = entity.get('status', 'Unknown')
            
            # --- Get new fields ---
            forecast_status = entity.get('crowdLevel', 'Unknown') # API uses 'crowdLevel'
            open_time = None
            close_time = None

            op_hours_list = entity.get('operatingHours', [])
            for schedule in op_hours_list:
                if schedule.get('type') == 'OPERATING':
                    open_time = schedule.get('startTime')
                    close_time = schedule.get('endTime')
                    break # Found the main schedule, stop looking
            
            park_data = {
                "name": name,
                "status": status,
                "forecast_status": forecast_status,
                "open_time": open_time,
                "close_time": close_time
            }
            park_data_list.append(park_data)
            
            print(f"Status check: {name} is {status}. Open: {open_time} Close: {close_time}")
    
    return park_data_list

def save_to_database(data, conn):
    """Saves the relevant ride data to the PostgreSQL database."""
    rides_processed = 0
    
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return

    # --- NEW LOGIC: Build a dedicated park_map as you suggested ---
    # We will look for both "PARK" and "THEME_PARK" to be safe
    park_entity_types = ["THEME_PARK", "PARK"]
    park_map = {}
    for entity in data['liveData']:
        if entity.get('entityType') in park_entity_types:
            park_map[entity['id']] = entity.get('name')
            
    if not park_map:
        print("--- DEBUG: park_map is EMPTY. No entities found with type 'PARK' or 'THEME_PARK'.")
    else:
        print(f"--- DEBUG: Built park_map with {len(park_map)} parks: {park_map}")
    # --- END NEW LOGIC ---

    try:
        with conn.cursor() as cursor:
            debug_rides_printed = 0 # Counter to limit debug spam
            for entity in data['liveData']:
                # We only want to save ATTRACTION entities
                if entity.get('entityType') == 'ATTRACTION':
                    
                    # --- NEW, SIMPLIFIED PARK NAME LOGIC ---
                    park_name = "Unknown" 
                    park_id = entity.get('parkId') # Get the direct parkId
                    
                    # Print debug info for the first 5 rides
                    if debug_rides_printed < 5: 
                        print(f"--- DEBUG: Ride '{entity.get('name')}' has parkId: '{park_id}'")
                        debug_rides_printed += 1

                    if park_id:
                        # Use the park_id to look up the name in our new map
                        park_name = park_map.get(park_id, "Unknown")
                        if park_name == "Unknown":
                            print(f"--- DEBUG: Warning! parkId '{park_id}' (for ride '{entity.get('name')}') was not found in the park_map.")
                    else:
                        if debug_rides_printed < 10: # Only print this warning a few times
                           print(f"--- DEBUG: Warning! Ride '{entity.get('name')}' is missing a parkId field.")
                           debug_rides_printed += 1 # Use same counter
                    # --- END NEW LOGIC ---
                    
                    ride_name = entity.get('name')
                    status = entity.get('status', 'Unknown')
                    
                    entity_tags = entity.get('tags', {})
                    attraction_type = entity_tags.get('type')
                    
                    wait_time = None
                    if 'queue' in entity and 'STANDBY' in entity['queue']:
                        wait_time = entity['queue']['STANDBY'].get('waitTime')
                    
                    if ride_name:
                        cursor.execute(
                            """
                            INSERT INTO wait_times (park_name, ride_name, wait_time_minutes, status, attraction_type)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (park_name, ride_name, wait_time, status, attraction_type)
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

import os
import requests
import psycopg2
import sys
from datetime import datetime, timezone
import json # Make sure this is still here
_debug_main_park_printed = False # This will help us print only once

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

def get_main_park_data(data):
    """
    Finds the 4 main theme parks and returns their full operating data.
    Returns a list of dictionaries.
    """
    global _debug_main_park_printed # Use the global flag
    
    main_park_names = [
        "Magic Kingdom Park",
        "Epcot",
        "Disney's Hollywood Studios",
        "Disney's Animal Kingdom Theme Park"
    ]
    park_data_list = []
    
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return []

    park_entity_types = ["THEME_PARK", "PARK"]

    for entity in data['liveData']:
        if entity.get('entityType') in park_entity_types and entity.get('name') in main_park_names:
            
            # --- NEW DEBUG LOGIC ---
            if not _debug_main_park_printed:
                print("\n\n--- DEBUG: FOUND A MAIN PARK ENTITY ---")
                try:
                    print(json.dumps(entity, indent=2))
                except Exception as e:
                    print(f"Error printing entity: {e}")
                print("---------------------------------------\n\n")
                _debug_main_park_printed = True
            # --- END DEBUG LOGIC ---

            name = entity['name']
            status = entity.get('status', 'Unknown')
            
            # These are the field names we need to verify
            forecast_status = entity.get('crowdLevel', 'Unknown') 
            open_time = None
            close_time = None

            op_hours_list = entity.get('operatingHours', [])
            for schedule in op_hours_list:
                if schedule.get('type') == 'OPERATING':
                    open_time = schedule.get('startTime')
                    close_time = schedule.get('endTime')
                    break
            
            park_data = {
                "name": name,
                "status": status,
                "forecast_status": forecast_status,
                "open_time": open_time,
                "close_time": close_time
            }
            park_data_list.append(park_data)
            
            # This print is still useful
            print(f"Status check: {name} is {status}. Open: {open_time} Close: {close_time}")
    
    return park_data_list

def get_main_park_data(data):
    """
    Finds the 4 main theme parks IN THE LIVE DATA
    and returns their current status for the "all parks closed" check.
    """
    main_park_names = [
        "Magic Kingdom Park",
        "Epcot",
        "Disney's Hollywood Studios",
        "Disney's Animal Kingdom Theme Park"
    ]
    park_statuses = {}
    
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return {}

    park_entity_types = ["THEME_PARK", "PARK"]

    for entity in data['liveData']:
        if entity.get('entityType') in park_entity_types and entity.get('name') in main_park_names:
            name = entity['name']
            status = entity.get('status', 'Unknown')
            park_statuses[name] = status
            print(f"Status check: {name} is {status}")
    
    return park_statuses

def save_to_database(data, conn):
    """Saves the relevant ride data to the PostgreSQL database."""
    rides_processed = 0
    
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return

    # --- Build the correct park_map ---
    park_entity_types = ["THEME_PARK", "PARK"]
    park_map = {}
    for entity in data['liveData']:
        if entity.get('entityType') in park_entity_types:
            park_map[entity['id']] = entity.get('name')
            
    if not park_map:
        print("Warning: park_map is EMPTY. No parks found in liveData.")
    else:
        print(f"Built park_map with {len(park_map)} parks from liveData.")

    try:
        with conn.cursor() as cursor:
            # Iterate over the LIVEDATA list for wait times
            for entity in data['liveData']:
                if entity.get('entityType') == 'ATTRACTION':
                    
                    park_name = "Unknown" 
                    park_id = entity.get('parkId')
                    
                    if park_id:
                        park_name = park_map.get(park_id, "Unknown")
                    
                    ride_name = entity.get('name')
                    status = entity.get('status')
                    
                    # --- FIX: Changed 'type' to 'event_type' ---
                    entity_tags = entity.get('tags', {})
                    attraction_type = entity_tags.get('event_type')
                    
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
        print(f"Successfully saved data for {rides_processed} rides. (Using 'event_type' for attraction_type)")
    
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
        
        # 1. Get live park statuses
        park_statuses = get_main_park_data(api_data)
        
        if park_statuses:
            # 2. Check if all parks are closed
            all_closed = all(status == 'CLOSED' for status in park_statuses.values())
            found_all_parks = len(park_statuses) == 4

            if found_all_parks and all_closed:
                print("All 4 main parks are reporting 'CLOSED'. Exiting script.")
                sys.exit(0) # Exit successfully
            elif not found_all_parks:
                print("Warning: Did not find all 4 main parks in liveData. Proceeding just in case.")
            else:
                print("At least one main park is open. Proceeding to save data.")
        else:
            print("Could not determine park statuses from liveData. Proceeding to save data.")

        try:
            with psycopg2.connect(DB_URL) as conn:
                print("Database connection successful.")
                
                # 3. Try to save the daily park data (hours/forecast)
                #    This now reads from api_data['schedule']
                save_daily_park_data(api_data, conn)
                
                # 4. Save the wait time data
                #    This now reads from api_data['liveData']
                save_to_database(api_data, conn)
                
        except psycopg2.OperationalError as e:
            print(f"Error connecting to database: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()

import os
import requests
import psycopg2
import sys
from datetime import datetime, timezone
import json 

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
    """
    DEBUGGING FUNCTION: This function will print the raw data for
    the FIRST 5 ENTITIES in the list, whatever they are.
    """
    
    if 'liveData' not in data or not data['liveData']:
        print("--- DEBUG: 'liveData' key is missing or the list is empty. ---")
        sys.exit(0) # Exit successfully

    print("--- STARTING NEW DEBUGGING MODE (Printing first 5 entities) ---")
    
    counter = 0
    for entity in data['liveData']:
        if counter < 5:
            print(f"\n\n--- ENTITY {counter + 1} (RAW DATA) ---")
            try:
                print(json.dumps(entity, indent=2))
            except Exception as e:
                print(f"Error printing entity: {e}")
                print(f"RAW ENTITY (unformatted): {entity}")
            print("--------------------------\n\n")
            counter += 1
        else:
            # Once we have 5, stop looping
            break
            
    print("--- DEBUGGING COMPLETE (Printed first 5 entities) ---")
    print("Please copy the 5 JSON blocks above from the log and paste them in the chat.")
    print("This script will now exit without saving to the database.")
    
    # We are not saving any data in this debug run.
    sys.exit(0)

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

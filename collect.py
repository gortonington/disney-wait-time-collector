import os
import requests
import psycopg2
import sys
from datetime import datetime, timezone
import json # Keep this just in case

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

    # The debug log confirmed 'PARK' is used. We'll check both just in case.
    park_entity_types = ["THEME_PARK", "PARK"]

    for entity in data['liveData']:
        if entity.get('entityType') in park_entity_types and entity.get('name') in main_park_names:
            name = entity['name']
            status = entity.get('status', 'Unknown')
            
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
            
            print(f"Status check: {name} is {status}. Open: {open_time} Close: {close_time}")
    
    return park_data_list

def save_daily_park_data(park_data_list, conn):
    """
    Saves the daily operating hours and forecast data.
    Uses "ON CONFLICT DO NOTHING" to ensure we only save one record
    per park, per day.
    """
    if not park_data_list:
        return

    print("Attempting to save daily park data...")
    saved_count = 0
    try:
        with conn.cursor() as cursor:
            for data in park_data_list:
                if data['open_time']:
                    # Convert ISO 8601 string to datetime object
                    data_date = datetime.fromisoformat(data['open_time']).date()
                else:
                    data_date = datetime.now(timezone.utc).date()

                cursor.execute(
                    """
                    INSERT INTO park_operating_data 
                        (data_date, park_name, open_time, close_time, forecast_status)
                    VALUES 
                        (%s, %s, %s, %s, %s)
                    ON CONFLICT (park_name, data_date) DO NOTHING;
                    """,
                    (
                        data_date,
                        data['name'],
                        data['open_time'],
                        data['close_time'],
                        data['forecast_status']
                    )
                )
                saved_count += cursor.rowcount
        
        conn.commit()
        if saved_count > 0:
            print(f"Successfully saved new daily data for {saved_count} parks.")
        else:
            print("Daily park data is already up-to-date.")
            
    except Exception as e:
        print(f"Error saving daily park data: {e}", file=sys.stderr)
        conn.rollback()

def save_to_database(data, conn):
    """Saves the relevant ride data to the PostgreSQL database."""
    rides_processed = 0
    
    if 'liveData' not in data:
        print("No 'liveData' key in API response.")
        return

    # --- Build the correct park_map ---
    # The debug log confirms entityType is 'PARK'
    park_entity_types = ["THEME_PARK", "PARK"]
    park_map = {}
    for entity in data['liveData']:
        if entity.get('entityType') in park_entity_types:
            park_map[entity['id']] = entity.get('name')
            
    if not park_map:
        print("Warning: park_map is EMPTY. No parks found.")
    else:
        print(f"Built park_map with {len(park_map)} parks.")

    try:
        with conn.cursor() as cursor:
            for entity in data['liveData']:
                # We only want to save ATTRACTION entities
                if entity.get('entityType') == 'ATTRACTION':
                    
                    # --- Correct Park Name Logic ---
                    park_name = "Unknown" 
                    park_id = entity.get('parkId') # Confirmed from log
                    
                    if park_id:
                        # Look up the park_id in our map
                        park_name = park_map.get(park_id, "Unknown")
                    
                    ride_name = entity.get('name')
                    status = entity.get('status')
                    
                    # --- Correct Attraction Type Logic ---
                    # This will be None if 'tags' or 'type' doesn't exist, which is correct
                    entity_tags = entity.get('tags', {})
                    attraction_type = entity_tags.get('type')
                    
                    # --- Wait Time Logic ---
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
        park_data_list = get_main_park_data(api_data)
        
        if park_data_list:
            all_closed = all(data['status'] == 'CLOSED' for data in park_data_list)
            found_all_parks = len(park_data_list) == 4

            if found_all_parks and all_closed:
                print("All 4 main parks are reporting 'CLOSED'. Exiting script.")
                sys.exit(0)
            elif not found_all_parks:
                print("Warning: Did not find all 4 main parks in API response. Proceeding just in case.")
            else:
                print("At least one main park is open. Proceeding to save data.")
        else:
            print("Could not determine park statuses. Proceeding to save data.")

        try:
            with psycopg2.connect(DB_URL) as conn:
                print("Database connection successful.")
                
                # Try to save the daily park data (hours/forecast)
                save_daily_park_data(park_data_list, conn)
                
                # Save the wait time data
                save_to_database(api_data, conn)
                
        except psycopg2.OperationalError as e:
            print(f"Error connecting to database: {e}", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            print(f"An unexpected error occurred: {e}", file=sys.stderr)
            sys.exit(1)

if __name__ == "__main__":
    main()

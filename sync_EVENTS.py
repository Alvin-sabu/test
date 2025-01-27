import os
import requests
import json
from datetime import datetime
from google.auth import exceptions
from google.oauth2 import service_account
from googleapiclient.discovery import build

def get_airtable_records():
    airtable_api_key = os.getenv('AIRTABLE_API_KEY')
    airtable_base_id = os.getenv('AIRTABLE_BASE_ID')
    airtable_table_name = os.getenv('AIRTABLE_TABLE_NAME')
    
    if not all([airtable_api_key, airtable_base_id, airtable_table_name]):
        raise ValueError("Missing required Airtable environment variables")

    url = f"https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}"
    headers = {"Authorization": f"Bearer {airtable_api_key}"}

    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json().get('records', [])

def filter_current_and_next_month_events(records):
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year

    # Calculate the next month and handle year rollover
    if current_month == 12:
        next_month = 1
        next_month_year = current_year + 1
    else:
        next_month = current_month + 1
        next_month_year = current_year

    due_events = []

    for record in records:
        next_billing_date = record['fields'].get('Next Billing Date')
        if next_billing_date:
            if isinstance(next_billing_date, list):
                next_billing_date = next_billing_date[0]

            try:
                next_billing_date_obj = datetime.strptime(next_billing_date, "%Y-%m-%d")
                
                # Check if the event is due in the current month or next month
                if (next_billing_date_obj.month == current_month and next_billing_date_obj.year == current_year) or \
                   (next_billing_date_obj.month == next_month and next_billing_date_obj.year == next_month_year):
                    due_events.append(record)
            except ValueError as e:
                print(f"Error parsing date {next_billing_date}: {e}")
                continue

    return due_events

def setup_google_calendar():
    credentials_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
    if not credentials_path or not os.path.exists(credentials_path):
        raise ValueError(f"Google credentials file not found at {credentials_path}")

    try:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/calendar"]
        )
        return build('calendar', 'v3', credentials=credentials)
    except Exception as e:
        raise ValueError(f"Error setting up Google Calendar: {e}")

def create_google_calendar_event(service, event_details):
    try:
        calendar_id = 'primary'  # Using primary calendar
        
        event = {
            'summary': event_details['summary'],
            'description': event_details.get('description', ''),
            'start': {
                'dateTime': event_details['start'],
                'timeZone': 'UTC',
            },
            'end': {
                'dateTime': event_details['end'],
                'timeZone': 'UTC',
            }
        }
        
        result = service.events().insert(calendarId=calendar_id, body=event).execute()
        print(f"Successfully created event: {event_details['summary']}")
        return result
    
    except HttpError as error:
        print(f"Error creating event: {error}")
        return None
def filter_due_events(records):
    """Filter events due in current or next month."""
    due_events = []
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    
    # Calculate next month and year
    next_month = current_month + 1 if current_month < 12 else 1
    next_month_year = current_year if current_month < 12 else current_year + 1
    
    for record in records:
        if 'Next Billing Date' in record['fields']:
            try:
                next_billing_date = record['fields']['Next Billing Date']
                next_billing_date_obj = datetime.strptime(next_billing_date, "%Y-%m-%d")
                
                # Check if event is due in current or next month
                if (next_billing_date_obj.month == current_month and 
                    next_billing_date_obj.year == current_year) or \
                   (next_billing_date_obj.month == next_month and 
                    next_billing_date_obj.year == next_month_year):
                    due_events.append(record)
            except ValueError as e:
                print(f"Error parsing date {next_billing_date}: {e}")
                continue
    
    print(f"Found {len(due_events)} events for current/next month")
    return due_events

def main():
    try:
        print("Starting event sync process...")
        service = setup_google_calendar()
        records = get_airtable_records()
        filtered_records = filter_due_events(records)
        
        successful_events = 0
        failed_events = 0
        
        for record in filtered_records:
            event_details = {
                'summary': record['fields'].get('Name', 'Untitled Event'),
                'start': record['fields'].get('Start'),
                'end': record['fields'].get('End'),
                'description': record['fields'].get('Description', '')
            }
            
            if create_google_calendar_event(service, event_details):
                successful_events += 1
            else:
                failed_events += 1
        
        print(f"Sync completed. Success: {successful_events}, Failed: {failed_events}")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
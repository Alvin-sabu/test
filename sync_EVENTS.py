import os
import requests
import json
from datetime import datetime, timedelta
from googleapiclient.errors import HttpError
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

def filter_due_events(records):
    """Filter events due in current or next month."""
    due_events = []
    current_date = datetime.now()
    current_month = current_date.month
    current_year = current_date.year
    
    next_month = current_month + 1 if current_month < 12 else 1
    next_month_year = current_year if current_month < 12 else current_year + 1
    
    for record in records:
        if 'Next Billing Date' in record['fields']:
            try:
                next_billing_date = record['fields']['Next Billing Date']
                if isinstance(next_billing_date, list):
                    next_billing_date = next_billing_date[0]
                if not isinstance(next_billing_date, str):
                    continue
                    
                next_billing_date_obj = datetime.strptime(next_billing_date, "%Y-%m-%d")
                
                if (next_billing_date_obj.month == current_month and 
                    next_billing_date_obj.year == current_year) or \
                   (next_billing_date_obj.month == next_month and 
                    next_billing_date_obj.year == next_month_year):
                    due_events.append(record)
            except (ValueError, TypeError) as e:
                print(f"Error parsing date {next_billing_date}: {e}")
                continue
    
    print(f"Found {len(due_events)} events for current/next month")
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

def create_google_calendar_event(service, record):
    try:
        billing_date = record['fields'].get('Next Billing Date')
        if isinstance(billing_date, list):
            billing_date = billing_date[0]
            
        # Create event start time at 9 AM
        start_time = datetime.strptime(f"{billing_date} 09:00", "%Y-%m-%d %H:%M")
        # Set duration to 30 minutes
        end_time = start_time + timedelta(minutes=30)
        
        event = {
            'summary': record['fields'].get('Name', 'Billing Reminder'),
            'description': record['fields'].get('Description', ''),
            'start': {
                'dateTime': start_time.strftime("%Y-%m-%dT%H:%M:%S"),
                'timeZone': 'America/New_York',
            },
            'end': {
                'dateTime': end_time.strftime("%Y-%m-%dT%H:%M:%S"),
                'timeZone': 'America/New_York',
            }
        }
        
        result = service.events().insert(calendarId='primary', body=event).execute()
        print(f"Successfully created event: {event['summary']}")
        return True
    
    except HttpError as error:
        print(f"Error creating event: {error}")
        return False

def main():
    try:
        print("Starting event sync process...")
        service = setup_google_calendar()
        records = get_airtable_records()
        filtered_records = filter_due_events(records)
        
        successful_events = 0
        failed_events = 0
        
        for record in filtered_records:
            if create_google_calendar_event(service, record):
                successful_events += 1
            else:
                failed_events += 1
        
        print(f"Sync completed. Success: {successful_events}, Failed: {failed_events}")
        
    except Exception as e:
        print(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()
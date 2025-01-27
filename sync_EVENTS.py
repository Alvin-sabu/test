import os
import requests
import json
from datetime import datetime
from google.auth import exceptions
from google.oauth2 import service_account
from googleapiclient.discovery import build

# Load Airtable data
airtable_api_key = os.getenv('AIRTABLE_API_KEY')
airtable_base_id = os.getenv('AIRTABLE_BASE_ID')
airtable_table_name = os.getenv('AIRTABLE_TABLE_NAME')
url = f"https://api.airtable.com/v0/{airtable_base_id}/{airtable_table_name}"
headers = {"Authorization": f"Bearer {airtable_api_key}"}

response = requests.get(url, headers=headers)
data = response.json()
records = data.get('records', [])

# Filter events based on the current month
current_date = datetime.now()
due_events = []

for record in records:
    next_billing_date = record['fields'].get('Next Billing Date')
    if next_billing_date:
        next_billing_date_obj = datetime.strptime(next_billing_date, "%Y-%m-%d")
        if next_billing_date_obj.month == current_date.month and next_billing_date_obj.year == current_date.year:
            due_events.append(record)

# Authenticate with Google Calendar
service_account_key_json = json.loads(os.getenv('SERVICE_ACCOUNT_KEY_JSON'))
credentials = service_account.Credentials.from_service_account_info(
    service_account_key_json,
    scopes=["https://www.googleapis.com/auth/calendar"]
)

service = build('calendar', 'v3', credentials=credentials)

def create_google_calendar_event(event_details):
    event = {
        'summary': event_details['Title'],
        'start': {
            'dateTime': event_details['Next Billing Date'] + "T09:00:00",  # Assume time is 9 AM
            'timeZone': 'UTC',
        },
        'end': {
            'dateTime': event_details['Next Billing Date'] + "T09:30:00",  # 30 minutes duration
            'timeZone': 'UTC',
        },
    }
    created_event = service.events().insert(calendarId='primary', body=event).execute()
    print(f"Event created: {created_event['summary']}")

# Create events in Google Calendar
for event in due_events:
    event_details = {
        "Title": event['fields']['Title'],
        "Next Billing Date": event['fields']['Next Billing Date']
    }
    create_google_calendar_event(event_details)

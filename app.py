import streamlit as st
import gspread
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
import psycopg2
import time
import threading

# Define the scope for Google Sheets
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']

# File paths
token_pickle = 'token.pickle'
credentials_file = 'credentials.json'  # Your OAuth2 credentials file

# PostgreSQL connection parameters
pg_hostname = 'localhost'
pg_database = 'super'  # Name of the PostgreSQL database
pg_username = 'postgres'
pg_password = 'iambatman@123'
pg_port_id = 5432  # Default PostgreSQL port

# Time interval to check for updates (in seconds)
POLL_INTERVAL = 10  # Poll every 10 seconds

# Stop event for monitoring
stop_event = threading.Event()

# Function to authenticate and return Google Sheets client
def get_google_sheets_client():
    creds = None

    # Check if token.pickle exists (this stores the access token and refresh token)
    if os.path.exists(token_pickle):
        with open(token_pickle, 'rb') as token:
            creds = pickle.load(token)

    # If there are no valid credentials available, ask the user to log in
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(credentials_file, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the credentials for the next run
        with open(token_pickle, 'wb') as token:
            pickle.dump(creds, token)

    # Authorize the client to interact with Google Sheets
    client = gspread.authorize(creds)
    return client

# Function to insert records in batches of 100
def insert_records_in_batches(cursor, insert_query, data, batch_size=100):
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cursor.executemany(insert_query, batch)

# Function to create or replace PostgreSQL table and insert data
def replace_pg_table_with_sheet_data(data):
    conn = psycopg2.connect(
        host=pg_hostname,
        database=pg_database,
        user=pg_username,
        password=pg_password,
        port=pg_port_id
    )
    cursor = conn.cursor()

    # Check if the table exists and create it if necessary
    headers = data[0]
    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS my_table (
        {', '.join([f'{header} TEXT' for header in headers])}
    );
    """)

    cursor.execute("TRUNCATE TABLE my_table;")

    insert_query = f"INSERT INTO my_table ({', '.join(headers)}) VALUES ({', '.join(['%s']*len(headers))});"
    insert_records_in_batches(cursor, insert_query, data[1:], batch_size=100)
    
    conn.commit()

    cursor.close()
    conn.close()

# Function to check if two datasets are the same
def is_data_changed(old_data, new_data):
    return old_data != new_data

# Function to monitor Google Sheets and update PostgreSQL table
def monitor_google_sheet(client, sheet_key, poll_interval):
    spreadsheet = client.open_by_key(sheet_key)
    sheet = spreadsheet.sheet1
    previous_data = []

    while not stop_event.is_set():
        current_data = sheet.get_all_values()
        if is_data_changed(previous_data, current_data):
            replace_pg_table_with_sheet_data(current_data)
            previous_data = current_data
        
        time.sleep(poll_interval)

    st.write("Monitoring ended.")

# Function to display data from PostgreSQL table
def display_pg_table_data():
    try:
        conn1 = psycopg2.connect(
            host=pg_hostname,
            database=pg_database,
            user=pg_username,
            password=pg_password,
            port=pg_port_id
        )
        cursor = conn1.cursor()
        
        cursor.execute("SELECT * FROM my_table;")
        rows = cursor.fetchall()

        st.write("Data in 'my_table':")
        for row in rows:
            st.write(row)

    except (Exception, psycopg2.DatabaseError) as error:
        st.error(f"Error: {error}")
    
    finally:
        if conn1 is not None:
            cursor.close()
            conn1.close()

# Streamlit UI
st.title("Real-time Google Sheets to PostgreSQL Updater")

# Input the Google Sheets key and poll interval
sheet_key = st.text_input("Google Sheet Key", value='1hdEXwHIG_46KwvFeNkovfouBCsaRrvJxPXy6bn8cJp4')
poll_interval = st.number_input("Polling Interval (seconds)", min_value=5, max_value=60, value=10)

# Get Google Sheets client
client = get_google_sheets_client()

# Display buttons beside each other
col1, col2, col3 = st.columns(3)

# Flag to track if monitoring is running
monitoring_thread = None

# Start the real-time update process using a separate thread
if col1.button("Start Monitoring"):
    if not stop_event.is_set() and monitoring_thread is None:
        stop_event.clear()
        monitoring_thread = threading.Thread(target=monitor_google_sheet, args=(client, sheet_key, poll_interval))
        monitoring_thread.start()
        st.write("Monitoring started...")

# Stop monitoring by setting the event
if col2.button("Stop Monitoring"):
    if monitoring_thread is not None:
        stop_event.set()
        monitoring_thread = None  # Reset thread variable
        st.write("Monitoring ended.")

# Button to display data from PostgreSQL table
if col3.button("Display Table Data"):
    display_pg_table_data()

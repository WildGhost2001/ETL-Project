import requests

# for batch data API request

# url = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events"

# response = requests.get(url)
# response.json()

# print(response.json())



# API requsest rate limit

import time

url = "https://api.github.com/rate_limit"

remaining = requests.get(url).json()["rate"]["remaining"]

print(remaining)

# if remaining == 0:
#     time.sleep(60)



# request data from github API

# url = "https://api.github.com/user"

# user_data = requests.get(url).json()

# print(user_data)


# Authentication

import os

# from google.colab import userdata

# API_TOKEN = userdata.get('ACCESS_TOKEN')

# headers = {"Authorization": f"Bearer {API_TOKEN}"}
# response = requests.get("https://api.github.com/user", headers=headers)
# print(response.json())



# Pagination

# url = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events?page=9"

# response = requests.get(url)
# print(response)

# print(response.links["next"]["url"])


url = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events"

while True:
    response = requests.get(url)
    data = response.json()
    # print(len(data))

    if "next" not in response.links:
        break

    url = response.links["next"]["url"]



# Normalisation

def events_getter():

    url = "https://api.github.com/repos/DataTalksClub/data-engineering-zoomcamp/events"

    while True:
        response = requests.get(url)
        data = response.json()
        yield data

        if "next" not in response.links:
          break

        url = response.links["next"]["url"]


events_pages = events_getter()

for events_page in events_pages:

    print(events_page)



# # event = events_page[0]
# # print(event)


def process_event(event):
    result = {}

    result["id"] = event["id"]
    result["type"] = event["type"]
    result["public"] = event["public"]
    result["created_at"] = event["created_at"]

    result["actor_id"] = event["actor"]["id"]
    result["actor_login"] = event["actor"]["login"]

    return result


processed_events = []

for event in events_page:
  processed_event = process_event(event)
  processed_events.append(processed_event)

print(processed_events)


# # Change data Formate

from datetime import datetime
# # #datetime.fromisoformat timestamp()

def process_event(event):
    result = {}

    result["id"] = event["id"]
    result["type"] = event["type"]
    result["public"] = event["public"]

    parsed_timestamp = datetime.fromisoformat(event["created_at"])
    result["created_at"] = parsed_timestamp.timestamp()

    result["actor_id"] = event["actor"]["id"]
    result["actor_login"] = event["actor"]["login"]

    return result

print(process_event(event))


# Find lenght of data

all_data = []

pages = events_getter()

for page in pages:
   all_data.extend(page)

print(len(all_data))


def process_event(event):
    result = {}

    result["id"] = event["id"]
    result["type"] = event["type"]
    result["public"] = event["public"]

    parsed_timestamp = datetime.fromisoformat(event["created_at"])
    result["created_at"] = parsed_timestamp.timestamp()

    result["actor_id"] = event["actor"]["id"]
    result["actor_login"] = event["actor"]["login"]

    topics = event.get("payload",{}).get("pull_request", {}).get("base", {}).get("repo",{}).get("topics", [])

    processed_topics = []
    for topic in topics:
        processed_topic = {
           "event_id": event["id"],
           "topic_name" : topic
        }
        processed_topics.append(processed_topic)

    return result, processed_topics

processed_events = []
processed_topics = []

for event in all_data:
    processed_event,topics = process_event(event)
    processed_events.append(processed_event)
    processed_topics.extend(topics)

print(processed_events[:5])
print(processed_topics[:5])


# Load data to database duckdb

# i. create a connection to the database
import duckdb
conn = duckdb.connect("github_events.db")


# ii. create the "github_events" table
conn.execute("""
CREATE TABLE IF NOT EXISTS github_events(
    id TEXT PRIMARY KEY,
    type TEXT,
    public BOOLEAN,
    created_at DOUBLE,
    actor_id BIGINT,
    actor_login TEXT
);
""")

flattened_data = [
    (
       record["id"],
       record["type"],
       record["public"],
       record["created_at"],
       record["actor_id"],
       record["actor_login"]
    )
    for record in processed_events
]


# iii. Insert the data into the "github_events" table
conn.executemany("""
INSERT INTO github_events (id, type, public, created_at, actor_id, actor_login)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (id) DO NOTHING;
""", flattened_data)

df = conn.execute("SELECT * FROM github_events").df()
print(df.head())

conn.close()



# Dynamic schema handling in DuckDB
# add new coloumn
def process_event(event):
    result = {}

    result["id"] = event["id"]
    result["type"] = event["type"]
    result["public"] = event["public"]

    parsed_timestamp = datetime.fromisoformat(event["created_at"])
    result["created_at"] = parsed_timestamp.timestamp()

    result["actor_id"] = event["actor"]["id"]
    result["actor_login"] = event["actor"]["login"]

    result["repo_id"] = event["repo"]["id"]

    topics = event.get("payload",{}).get("pull_request", {}).get("base", {}).get("repo",{}).get("topics", [])

    processed_topics = []
    for topic in topics:
        processed_topic = {
           "event_id": event["id"],
           "topic_name" : topic
        }
        processed_topics.append(processed_topic)

    return result, processed_topics


processed_events = []
processed_topics = []

for event in all_data:
    processed_event,topics = process_event(event)
    processed_events.append(processed_event)
    processed_topics.extend(topics)

print(processed_events[:5])
print(processed_topics[:5])


# 1. Create a connection to DuckDB
conn = duckdb.connect("github_events.db")


# 2. Fetch current table schema
current_columns = {row[1] for row in conn.execute("PRAGMA table_info(github_events)").fetchall()}
print(current_columns)


# 3. Detect and add new columns dynamically
for record in processed_events:
    for key in record.keys():
        if key not in current_columns:
            col_type = "TEXT"          # Default type
            if isinstance(record[key], bool):
                col_type = "BOOLEAN"
            elif isinstance(record[key], int):
                col_type = "BIGINT"
            elif isinstance(record[key], float):
                col_type = "DOUBLE"
            print(f"ALTER TABLE github_events ADD COLUMN {key} {col_type};")
            alter_query = f"ALTER TABLE github_events ADD COLUMN {key} {col_type};"
            conn.execute(alter_query)
            print(f"Added new column: {key} ({col_type})")
            current_columns.add(key)       # Update schema tracking


# 4. Prepare data for insertion (handle missing fields)

columns = sorted(current_columns)      # Maintain consistent order
flattened_data = [
    tuple(record.get(col, None) for col in columns)  # Fill missing values with NULL
    for record in processed_events
]

# 5. Construct dynamic SQL for insertion
placeholders = ", ".join(["?" for _ in columns])
columns_str = ", ".join(columns)

insert_query = f"""
INSERT INTO github_events ({columns_str})
VALUES ({placeholders})
ON CONFLICT (id) DO UPDATE SET {", ".join(f"{col}=excluded.{col}" for col in columns if col != "id")};
"""

# 6. Insert data into DuckDB
conn.executemany(insert_query, flattened_data)


# 7. Query the table
df = conn.execute("""SELECT * FROM github_events""").df()
print(df)

conn.close()

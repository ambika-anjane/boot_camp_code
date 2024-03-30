import json
import psycopg2
from datetime import datetime
from dbt_artifacts_parser.parser import parse_run_results_v1

# Open and parse the run_results.json file
with open('C:/Users/ambik/ambik_backup/dbt_sample/test_test/target/run_results.json', 'r') as fp:
    run_results_dict = json.load(fp)
    run_results_obj = parse_run_results_v1(run_results=run_results_dict)
    print(run_results_obj.results[0])

# Establish a connection to the PostgreSQL database

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)
# Create a cursor object to execute SQL queries
cur = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS results_table (
    id SERIAL PRIMARY KEY,
    status VARCHAR(255),
    timing JSONB,
    thread_id VARCHAR(255),
    execution_time FLOAT,
    unique_id VARCHAR(255)
)
"""

# Execute the SQL query to create the table
cur.execute(create_table_query)

# Commit the transaction
conn.commit()

# Close the cursor and connection


# Define a function to convert datetime objects to string format
def convert_datetime_to_string(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()



# Define a function to insert the results into the PostgreSQL table
# def insert_results(results_obj):
#     for result in results_obj:
#         cur.execute(
#             "INSERT INTO results_table (status, timing, thread_id, execution_time, unique_id) VALUES (%s, %s, %s, %s, %s)",
#             (
#                 result.status.value,
#                 json.dumps([timing.__dict__ for timing in result.timing], default=convert_datetime_to_string),  # Convert timing list to JSON string
#                 result.thread_id,
#                 result.execution_time,
#                 result.unique_id
#             )
#         )

def insert_results(results_obj):
    for result in results_obj:
        timing_dicts = []  # List to store inner dictionaries of timing objects
        for timing in result.timing:
            timing_dict = timing.__dict__
            timing_dicts.append(timing_dict)
        
        cur.execute(
            "INSERT INTO results_table (status, timing, thread_id, execution_time, unique_id) VALUES (%s, %s, %s, %s, %s)",
            (
                result.status.value,
                json.dumps(timing_dicts, default=convert_datetime_to_string),  # Convert timing list to JSON string
                result.thread_id,
                result.execution_time,
                result.unique_id
            )
        )
# Insert results into the PostgreSQL table
insert_results(run_results_obj.results)

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

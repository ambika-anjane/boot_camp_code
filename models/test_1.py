import json
import psycopg2


def flatten_json(data, parent_key='', sep='_'):
    """
    Flatten JSON data into a flat dictionary.

    Args:
    data (dict): The JSON data to flatten.
    parent_key (str): The parent key used for recursion.
    sep (str): The separator used to join keys.

    Returns:
    dict: The flattened dictionary.
    """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# Read JSON data from file
with open('C:/Users/ambik/ambik_backup/dbt_sample/test_test/target/run_results.json', 'r') as file:
    json_data = json.load(file)

# Flatten JSON data
flattened_data = flatten_json(json_data)

# # Write flattened data to a text file
# with open('flattened_run_results.txt', 'w') as file:
#     for key, value in flattened_data.items():
#         file.write(f"{key}: {value}\n")

# print("Flattened data has been written to flattened_run_results.txt")


conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="postgres"
)

# Create a cursor object using the connection
cur = conn.cursor()

# Create a table to store the flattened data
cur.execute("""
    CREATE TABLE IF NOT EXISTS flattened_data (
        key TEXT PRIMARY KEY,
        value TEXT
    )
""")

# Insert flattened data into the database table
for key, value in flattened_data.items():
    cur.execute("INSERT INTO flattened_data (key, value) VALUES (%s, %s)", (key, json.dumps(value)))

# Commit the transaction
conn.commit()

# Close the cursor and connection
cur.close()
conn.close()

print("Flattened data has been written to the PostgreSQL database table.")
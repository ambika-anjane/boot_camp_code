import sqlite3
from datetime import datetime
from run_results_v1 import BaseArtifactMetadata
metadata_obj = BaseArtifactMetadata(
    dbt_schema_version='1.0',
    dbt_version='0.20.0',
    generated_at=datetime.now(),
    invocation_id='123456789',
    env={'key1': 'value1', 'key2': 'value2'}
)

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect('metadata.db')
cursor = conn.cursor()

# Define the SQLite table schema
create_table_sql = """
CREATE TABLE IF NOT EXISTS metadata (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dbt_schema_version TEXT,
    dbt_version TEXT,
    generated_at TEXT,
    invocation_id TEXT,
    env TEXT
);
"""

# Execute the SQL to create the table
cursor.execute(create_table_sql)

# Insert data into the table
insert_sql = """
INSERT INTO metadata (dbt_schema_version, dbt_version, generated_at, invocation_id, env)
VALUES (?, ?, ?, ?, ?);
"""
metadata_values = (
    metadata_obj.dbt_schema_version,
    metadata_obj.dbt_version,
    str(metadata_obj.generated_at),
    metadata_obj.invocation_id,
    str(metadata_obj.env)
)
cursor.execute(insert_sql, metadata_values)

# Commit changes and close the connection
conn.commit()
conn.close()

print("Data has been inserted into the database.")

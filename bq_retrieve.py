from google.cloud import bigquery

project_id = "bsky-store"
dataset_id = "bsky_data"
table_name = "Posts"

client = bigquery.Client()

# Define your query
query = f"""
    SELECT * FROM `{project_id}.{dataset_id}.{table_name}`
    LIMIT 100
"""

# Run the query
query_job = client.query(query)

# Fetch results into a DataFrame
df = query_job.to_dataframe()

# Display or work with the DataFrame
df.to_csv('output/current.csv', index=False)
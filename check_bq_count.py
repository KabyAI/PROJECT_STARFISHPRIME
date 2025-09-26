from google.cloud import bigquery

project_id = "project-starfishprime-001"
client = bigquery.Client(project=project_id, location="europe-north2")

query = "SELECT COUNT(*) as count FROM `project-starfishprime-001.silver.openmeteo_daily_ca`"

try:
    query_job = client.query(query)
    results = query_job.result()
    for row in results:
        print(f"Row count: {row.count}")
except Exception as e:
    print(f"An error occurred: {e}")

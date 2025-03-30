from google.cloud import bigquery
import os

client = bigquery.Client(project='humptydumptysatonawall')

# ✅ Correct target_table format (no SQL query here)
target_table = 'humptydumptysatonawall.sample_data.city_housing'

job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    write_disposition='WRITE_TRUNCATE'
)

# write_disposition='WRITE_APPEND'


cur_path = os.getcwd()
file = 'House_price_tf_fuc.csv'
file_path = os.path.join(cur_path, 'data_files', file)

# Load the CSV file to BigQuery
with open(file_path, 'rb') as source_file:
    load_job = client.load_table_from_file(
        source_file,
        target_table,  # Now it's the correct table reference
        job_config=job_config
    )

load_job.result()  # Wait for the load job to complete

# ✅ Correct way to get the destination table
destination_table = client.get_table(target_table)
print(f'You have {destination_table.num_rows} rows in your table.')

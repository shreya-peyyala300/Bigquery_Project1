from google.cloud import bigquery

client=bigquery.Client(project='')
query="""SELECT * 
    FROM oscar_sql.city_house_price;"""

query_job=client.query(query)
results=query_job.result()
for r in results:
    print(r.year,r.country)

# Imports the Google Cloud client library
from google.cloud import bigquery
import uuid
import json
import create_table_a1;
import utils

# table config
dataset_id = 'my_new_dataset'
a1_table_id = 'a1_results'
project_id = 'candidate-eval'

def fetch_a1_results():
    client = bigquery.Client()
    query_job = client.run_async_query(str(uuid.uuid4()), """
        #standardSQL
		SELECT a.id
		FROM `bigquery-public-data.stackoverflow.users` AS a
		LEFT JOIN `bigquery-public-data.stackoverflow.stackoverflow_posts` AS b ON (a.id = b.owner_user_id)
		WHERE a.reputation > 400000
		  AND EXTRACT(YEAR
		              FROM b.last_activity_date) = 2016
		  AND (REGEXP_CONTAINS(b.tags, r'(java|python)(\||$)'))
		GROUP BY a.id
		LIMIT 12       
        """)

    print 'fetching result'
    query_job.begin()
    query_job.result()  # Wait for job to complete.

    destination_table = query_job.destination
    destination_table.reload()
    
    data = []
    for row in destination_table.fetch_data():
    	data.append([row[0]])
    
    # creating table
    create_table_a1.create_table_a1(dataset_id, a1_table_id, project_id)
    
    # streaming data into table
    print 'inserting data into table'
    utils.stream_data(dataset_id, a1_table_id, data, project_id)
    



if __name__ == '__main__':
	fetch_a1_results()
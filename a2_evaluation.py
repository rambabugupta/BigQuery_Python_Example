# Imports the Google Cloud client library
from google.cloud import bigquery
import uuid
import json
import create_table_a2;
import utils

# table config
dataset_id = 'my_new_dataset'
a2_table_id = 'a2_results'
project_id = 'candidate-eval'


def fetch_a2_results():
    client = bigquery.Client()
    query_job = client.run_async_query(str(uuid.uuid4()), """
        #standardSQL
        with tiers as (SELECT owner_user_id user_id,
               CASE
                   WHEN pos BETWEEN 1 AND 3 THEN 1
                   WHEN pos BETWEEN 4 AND 10 THEN 2
                   ELSE 3
               END tier,
               SUM(favorite_count) favorite_count
        FROM
          ( SELECT t.owner_user_id,
                   t.favorite_count,
                   ROW_NUMBER() OVER(PARTITION BY t.owner_user_id
                                     ORDER BY t.favorite_count DESC) pos
           FROM
             (SELECT owner_user_id,
                     post_type_id,
                     sum(favorite_count) favorite_count
              FROM `bigquery-public-data.stackoverflow.stackoverflow_posts`
              WHERE owner_user_id IN
                  (SELECT id
                   FROM `candidate-eval.my_new_dataset.a1_results`)
              GROUP BY owner_user_id,
                       post_type_id
              ORDER BY favorite_count DESC) t)
        GROUP BY user_id, tier
        ORDER BY user_id, tier)
        SELECT user_id , tier, 
        ROUND(favorite_count / SUM(favorite_count) OVER(PARTITION BY user_id), 3) share
        FROM tiers
    """)

    print 'fetching result'
    query_job.begin()
    query_job.result()  # Wait for job to complete.

    destination_table = query_job.destination
    destination_table.reload()
    
    data = []
    for row in destination_table.fetch_data():
        t_r = []
        t_r.append(row[0])
        t_r.append(row[1])
        if row[2]:
            t_r.append(row[2])
        else:
            t_r.append(0)
        data.append(t_r)
    
    # creating table
    create_table_a2.create_table_a2(dataset_id, a2_table_id, project_id)
    
    #streaming data in table
    print 'inserting data into table'
    utils.stream_data(dataset_id, a2_table_id, data, project_id)
    

# part A2 Execution
if __name__ == '__main__':
	fetch_a2_results()
from google.cloud import bigquery
import uuid
import json

# function[stream_data] will stream the data into destination table
def stream_data(dataset_name, table_name, data, project=None):
    bigquery_client = bigquery.Client(project=project)
    dataset = bigquery_client.dataset(dataset_name)
    table = dataset.table(table_name)
    table.reload()
    rows = data
    errors = table.insert_data(rows)

    if not errors:
        print('Loaded 1 row into {}:{}'.format(dataset_name, table_name))
    else:
        print('Errors:')
        print(errors)


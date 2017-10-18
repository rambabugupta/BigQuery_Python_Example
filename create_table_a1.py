from google.cloud import bigquery
import uuid

# this function will create table if not exists otherwise
# it will delete the table and recreate it

def create_table_a1(dataset_name, table_name, project=None):
    bigquery_client = bigquery.Client(project=project)
    dataset = bigquery_client.dataset(dataset_name)
    if not dataset.exists():
        print('Dataset {} does not exist.'.format(dataset_name))
        return
    
    table = dataset.table(table_name)

    if table.exists():
        print 'Deleting Existing table'
        table.delete();

    table = dataset.table(table_name)

    table.schema = (
        bigquery.SchemaField('id', 'INTEGER'),
    )

    try:
        table.create()
        print('Created table {} in dataset {}.'.format(table_name, dataset_name))
    except Exception as e:
            print ('Error in creating table', e)


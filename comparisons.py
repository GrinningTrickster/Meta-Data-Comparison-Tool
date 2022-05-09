from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from google.cloud.bigquery import DatasetReference
from google.oauth2 import service_account
from fuzzywuzzy import fuzz, process

# insert the full path location for the JSON file containing the service account credentials.
credentials = service_account.Credentials.from_service_account_file(r'C:\Python\Credentials\Big Query\credentials.json')

schema = [
    bigquery.SchemaField('url', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('description', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('h1', 'STRING', mode='NULLABLE'),
]

comparison_schema = [
    bigquery.SchemaField('url', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('previous_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('latest_date', 'DATE', mode='NULLABLE'),
    bigquery.SchemaField('previous_title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('latest_title', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('title_similarity_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('title_partial_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('title_token_sort_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('previous_description', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('latest_description', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('description_similarity_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('description_partial_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('description_token_sort_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('previous_h1', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('latest_h1', 'STRING', mode='NULLABLE'),
    bigquery.SchemaField('h1_similarity_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('h1_partial_ratio', 'INTEGER', mode='NULLABLE'),
    bigquery.SchemaField('h1_token_sort_ratio', 'INTEGER', mode='NULLABLE'),
]


def bq_create_table(project_id, data_set, table_ref):
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    dataset_ref = DatasetReference(project_id, data_set)

    # Prepares a reference to the table
    table_ref = dataset_ref.table(table_ref)

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))


# add meta data from list into big query
def addmetadata(project_id, dataset, tableref, date, url, title, description, h1):
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    dataset_ref = DatasetReference(project_id, dataset)
    table_ref = dataset_ref.table(tableref)
    try:
        bigquery_client.get_table(table_ref)
        rows_to_insert = (url, date, title, description, h1)
        insert = bigquery_client.insert_rows(table_ref, [rows_to_insert], selected_fields = schema)
    except NotFound:
        print('table could not be found and data could not be inserted.')


def latestmetadataresult(project_id, dataset, tableref):
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    table = (dataset + "." + tableref)
    try:
        # takes last date
        sql = """
        SELECT * FROM """ + table + """
        WHERE date IN (SELECT max(date) FROM """ + table + """);"""
        df = bigquery_client.query(sql).to_dataframe()
    except NotFound:
        print('table could not be found and data could not be queried.')
    return df


def previousmetadataresult(project_id, dataset, tableref):
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    table = (dataset + "." + tableref)
    try:
        # takes second last date
        sql = """
        SELECT * FROM """ + table + """
        WHERE date = ( SELECT MAX(date) FROM """ + table + """
               WHERE date < ( SELECT MAX(date)
                              FROM """ + table + """
                            )
             ) ;"""
        df2 = bigquery_client.query(sql).to_dataframe()
    except NotFound:
        print('table could not be found and data could not be queried.')
    return df2


def comparemetadata(project_id, dataset, tableref, newtableref):
    latest = latestmetadataresult(project_id, dataset, tableref)
    prev = previousmetadataresult(project_id, dataset, tableref)
    merged = latest.merge(prev, on="url", suffixes=('_current', '_prior'))
    mergedlist = merged.values.tolist()

    # add to big query
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    dataset_ref = DatasetReference(project_id, dataset)
    newtableref = dataset_ref.table(newtableref)

    try:
        bigquery_client.get_table(newtableref)
    except NotFound:
        table = bigquery.Table(newtableref, schema=comparison_schema)
        table = bigquery_client.create_table(table)
        bigquery_client.get_table(newtableref)

    # end of big query reference

    for row in mergedlist:
        titlesim = fuzz.ratio(row[2],row[6])
        titlepartsim = fuzz.partial_ratio(row[2],row[6])
        titletokensort = fuzz.token_sort_ratio(row[2],row[6])
        descriptionsim = fuzz.ratio(row[3], row[7])
        descpartsim = fuzz.partial_ratio(row[3], row[7])
        desctokensort = fuzz.token_sort_ratio(row[3], row[7])
        h1sim = fuzz.ratio(row[4],row[8])
        h1partialsim = fuzz.partial_ratio(row[4],row[8])
        h1tokensort = fuzz.token_sort_ratio(row[4],row[8])
        finalrow = (row[0],
                    row[5], row[1],
                    row[6],row[2], titlesim,titlepartsim,titletokensort,
                    row[7], row[3], descriptionsim, descpartsim, desctokensort,
                    row[8], row[4], h1sim, h1partialsim, h1tokensort)
        insert = bigquery_client.insert_rows(newtableref, [finalrow], selected_fields=comparison_schema)
        print(finalrow)
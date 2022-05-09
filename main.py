import pandas as pd
from crawler import html_request, make_soup
from meta_data import get_title, get_description, get_h1
from comparisons import bq_create_table, comparemetadata, addmetadata
from datetime import datetime


if __name__ == '__main__':

    # project_id, dataset, tableref and comparison_tableref refers to the data's location in Big Query
    project_id = 'project_name'
    dataset = 'dataset_name'

    # meta data table name
    tableref = 'table_name'

    date = datetime.today().strftime('%Y-%m-%d')

    # create table if not created
    bq_create_table(project_id, dataset, tableref)

    # .txt file with URLs for acquiring meta data
    filename = 'urls.txt'
    with open(filename) as f:
        content = f.readlines()
    content = [x.strip() for x in content]

    for page in content:
        url, html, status = html_request(page)
        soup = make_soup(html)
        title = get_title(soup)
        description = get_description(soup)
        h1 = get_h1(soup)
        addmetadata(project_id, dataset, tableref, date, url, title, description, h1)


    # meta data comparison
    comparison_tableref = 'compare_meta_data'
    merge = comparemetadata(project_id, dataset, tableref, comparison_tableref)






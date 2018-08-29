# On stat1005
# source python_virtualenv/edit_revert/bin/activate
# deactivate

import sys
import json
import datetime
import mwdb
import mwreverts.db
import requests
import pandas as pd
import mysql.connector as mysql

# Get a list of active wikis

response = requests.get(
    'https://www.mediawiki.org/w/api.php?action=sitematrix&format=json&smtype=language&formatversion=2')
site_matrix = json.loads(response.text)['sitematrix']
if 'count' in site_matrix:
    del site_matrix['count']

active_wikis = pd.DataFrame([])
for index in site_matrix.keys():
    df = pd.DataFrame(
        columns=[
            'code',
            'sitename',
            'url',
            'dbname',
            'closed'],
        data=site_matrix[index]['site'])
    df['language_code'] = site_matrix[index]['code']
    df['language_name'] = site_matrix[index]['localname']
    active_wikis = active_wikis.append(df, ignore_index=True)

active_wikis = active_wikis[(active_wikis.code == 'wiki') & (
    active_wikis.closed.isnull())]  # only keep active wikipedia
del active_wikis['code']
del active_wikis['closed']

# Query all the revisions and fetch revert information

query_vars = dict(
    start_date='20180701',
    end_date=datetime.datetime.today().strftime('%Y%m%d'))
query = """
SELECT
DATE(LEFT(rev_timestamp, 8)) AS `date`,
rev_id,
rev_user AS `local_user_id`,
page_namespace,
page_id
FROM revision
INNER JOIN change_tag ON rev_id = ct_rev_id AND ct_tag = 'ios app edit'
LEFT JOIN page ON rev_page = page_id
WHERE rev_timestamp >= '{start_date}'
AND rev_timestamp < '{end_date}'
;
"""
query = query.format(**query_vars)

all_edits = pd.DataFrame([])
for wiki in active_wikis.dbname:
    conn = mysql.connect(
        host='analytics-store.eqiad.wmnet',
        option_files='/etc/mysql/conf.d/analytics-research-client.cnf',
        database=wiki,
        autocommit=True
    )

    try:
        df = pd.read_sql_query(query, conn)
        df['wiki'] = wiki
        df['is_reverted'] = None

        # fetch revert info
        schema = mwdb.Schema(
            'mysql+pymysql://analytics-store.eqiad.wmnet/' +
            wiki +
            '?read_default_file=/etc/mysql/conf.d/analytics-research-client.cnf')

        for row in df.itertuples():
            try:
                _, reverted, _ = mwreverts.db.check(
                    schema, rev_id=row.rev_id, page_id=row.page_id,
                    radius=5, window=48 * 60 * 60)
                df.at[row.Index, 'is_reverted'] = reverted is not None
            except RuntimeError as e:
                sys.stderr.write(str(e))
                continue

        # append to all_edits data frame
        all_edits = all_edits.append(df, ignore_index=True)
        print(wiki + ' completed!')
    except TypeError:
        print(wiki + ' failed!')
        continue

    conn.close()

all_edits.to_csv('data/all_edits.tsv', sep='\t', quoting=csv.QUOTE_NONE)

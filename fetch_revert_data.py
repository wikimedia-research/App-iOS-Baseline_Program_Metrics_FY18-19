#!/home/chelsyx/python_virtualenv/edit_revert/bin/python

import sys
import json
import csv
import datetime
import time
import mwdb
import mwapi
import mwreverts.db
import mwreverts.api
import requests
import pandas as pd
import mysql.connector as mysql
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool
from functools import partial


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

# Functions for checking revert info


def check_reverted_db(schema, rev_id, page_id):
    _, reverted, _ = mwreverts.db.check(
        schema, rev_id=rev_id, page_id=page_id,
        radius=5, window=48 * 60 * 60)
    return (reverted is not None)


def check_reverted_api(api_session, rev_id, page_id):
    _, reverted, _ = mwreverts.api.check(
        api_session, rev_id=rev_id, page_id=page_id,
        radius=5, window=48 * 60 * 60)
    return (reverted is not None)


def check_reverted(wiki, api_session, rev_id, page_id, timeout):
    # Use mwreverts.api.check first. If not work, use mwreverts.db.check
    try:
        out = check_reverted_api(api_session, rev_id, page_id)
        return out
    except mwapi.errors.APIError:
        print(
            "API error: revision " +
            str(rev_id) +
            '. Use mwreverts.db.check instead.')
        try:
            schema = mwdb.Schema(
                'mysql+pymysql://analytics-store.eqiad.wmnet/' + wiki +
                '?read_default_file=/etc/mysql/conf.d/analytics-research-client.cnf')
            p = ThreadPool(1)
            res = p.apply_async(
                check_reverted_db,
                kwds={
                    'schema': schema,
                    'rev_id': rev_id,
                    'page_id': page_id})
            # Wait timeout seconds for check_reverted_db to complete.
            out = res.get(timeout)
            p.close()
            p.join()
            return out
        except multiprocessing.TimeoutError:
            print("mwreverts.db.check timeout: failed to retrieve revert info for revision " + str(rev_id))
            p.terminate()  # kill mwreverts.db.check if it runs more than timeout seconds
            p.join()
            return None


def iter_check_reverted(df, wiki, api_session, timeout):
    for row in df.itertuples():
        try:
            df.at[row.Index, 'is_reverted'] = check_reverted(
                wiki, api_session, row.rev_id, row.page_id, timeout)
        except RuntimeError:
            print('Failed to get revert info for revision ' + str(row.rev_id))
            continue
    return df

# Loop through all active wikis


all_edits = pd.DataFrame([])
for wiki in active_wikis.dbname:

    print(wiki + ' start!')
    init = time.perf_counter()
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

        api_session = mwapi.Session(active_wikis.url[active_wikis.dbname == wiki].to_string(
            index=False), user_agent="Revert detection demo <cxie@wikimedia.org>")
        timeout = 10  # kill mwreverts.db.check if it runs more than 10 seconds

        # fetch revert info for each revision
        num_processes = 8  # use 8 processes
        if df.shape[0] >= 1000:  # if the data frame has more than 1000 rows, use multiprocessing
            chunk_size = int(df.shape[0] / num_processes)
            chunks = [df.ix[df.index[i:i + chunk_size]]
                      for i in range(0, df.shape[0], chunk_size)]

            pool = multiprocessing.Pool(processes=num_processes)
            result = pool.map(
                partial(
                    iter_check_reverted,
                    wiki=wiki,
                    api_session=api_session,
                    timeout=timeout),
                chunks)
            for i in range(len(result)):
                df.ix[result[i].index] = result[i]

            pool.close()
            pool.join()
        else:
            df = iter_check_reverted(df, wiki, api_session, timeout=timeout)

        # append to all_edits data frame
        all_edits = all_edits.append(df, ignore_index=True)
        elapsed = time.perf_counter() - init
        print("{} completed in {:0.0f} s".format(wiki, elapsed))
    except TypeError:
        print(wiki + ' failed!')
        pass

    conn.close()

all_edits.to_csv('data/all_edits.tsv', sep='\t', index=False, quoting=csv.QUOTE_NONE)

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
`date`,
ios_app_edits.local_user_id AS local_user_id,
IFNULL(previous_app_editors.new_ios_editor, 'TRUE') AS new_ios_editor,
rev_id,
namespace,
page_id,
is_deleted
FROM (
-- Edits made with iOS app on visible pages:
SELECT
DATE(LEFT(rev_timestamp, 8)) AS `date`,
rev_user AS local_user_id,
rev_id,
page.page_namespace AS namespace,
page_id,
FALSE AS is_deleted
FROM change_tag
INNER JOIN revision ON (
revision.rev_id = change_tag.ct_rev_id
AND revision.rev_timestamp >= '{start_date}'
AND revision.rev_timestamp < '{end_date}'
AND change_tag.ct_tag = 'ios app edit'
)
LEFT JOIN page ON revision.rev_page = page.page_id
UNION ALL
-- Edits made with iOS app on deleted pages:
SELECT
DATE(LEFT(ar_timestamp, 8)) AS `date`,
ar_user AS local_user_id,
ar_rev_id AS rev_id,
ar_namespace AS namespace,
ar_page_id AS page_id,
TRUE AS is_deleted
FROM change_tag
INNER JOIN archive ON (
archive.ar_rev_id = change_tag.ct_rev_id
AND archive.ar_timestamp >= '{start_date}'
AND archive.ar_timestamp < '{end_date}'
AND change_tag.ct_tag = 'ios app edit'
)
) AS ios_app_edits
LEFT JOIN (
-- Editors who have used a mobile app (Android/iOS)
-- to edit Wikipedia before the start_date:
SELECT DISTINCT local_user_id,
'FALSE' as new_ios_editor
FROM (
-- Editors who have previously used a mobile app to edit visible pages:
SELECT rev_user AS local_user_id
FROM change_tag
INNER JOIN revision ON (
revision.rev_id = change_tag.ct_rev_id
AND revision.rev_timestamp < '{start_date}'
AND change_tag.ct_tag = 'mobile app edit'
)
UNION ALL
-- Editors who have previously used a mobile app to edit deleted pages:
SELECT ar_user AS local_user_id
FROM change_tag
INNER JOIN archive ON (
archive.ar_rev_id = change_tag.ct_rev_id
AND archive.ar_timestamp < '{start_date}'
AND change_tag.ct_tag = 'mobile app edit'
)
) AS combined_revisions
) AS previous_app_editors
ON previous_app_editors.local_user_id = ios_app_edits.local_user_id
;
"""
query = query.format(**query_vars)

# Functions for checking revert info


def check_reverted_db(wiki, rev_id, page_id, is_deleted):
    # Checks the revert status of a regular revision
    def check_regular(schema, rev_id, page_id):
        _, reverted, _ = mwreverts.db.check(
            schema, rev_id=rev_id, page_id=page_id,
            radius=5, window=48 * 60 * 60)
        return (reverted is not None)

    # Checks the revert status of an archived revision
    def check_archive(schema, rev_id):
        _, reverted, _ = mwreverts.db.check_archive(
            schema, rev_id=rev_id, radius=5, window=48 * 60 * 60)
        return (reverted is not None)

    try:
        schema = mwdb.Schema(
            'mysql+pymysql://analytics-store.eqiad.wmnet/' + wiki +
            '?read_default_file=/etc/mysql/conf.d/analytics-research-client.cnf')
        p = ThreadPool(1)
        if is_deleted:
            res = p.apply_async(
                check_archive,
                kwds={
                    'schema': schema,
                    'rev_id': rev_id})
        else:
            res = p.apply_async(
                check_regular,
                kwds={
                    'schema': schema,
                    'rev_id': rev_id,
                    'page_id': page_id})
        # Wait timeout seconds for check_archive or check_regular to complete.
        out = res.get(timeout)
        p.close()
        p.join()
        return out
    except multiprocessing.TimeoutError:
        print(
            "mwreverts.db.check timeout: failed to retrieve revert info for revision " +
            str(rev_id))
        p.terminate()  # kill mwreverts.db.check if it runs more than timeout seconds
        p.join()
        return None


def check_reverted_api(api_session, rev_id, page_id):
    _, reverted, _ = mwreverts.api.check(
        api_session, rev_id=rev_id, page_id=page_id,
        radius=5, window=48 * 60 * 60)
    return (reverted is not None)


def check_reverted(wiki, api_session, rev_id, page_id, timeout):
    # Use mwreverts.api.check first. If not work, use check_reverted_db
    try:
        out = check_reverted_api(api_session, rev_id, page_id)
        return out
    except mwapi.errors.APIError:
        print(
            "API error: revision " +
            str(rev_id) +
            '. Use mwreverts.db.check instead.')
        return check_reverted_db(wiki, rev_id, page_id, is_deleted=False)


def iter_check_reverted(df, wiki, api_session, timeout):
    for row in df.itertuples():
        try:
            if row.is_deleted:
                df.at[row.Index, 'is_reverted'] = check_reverted_db(
                    wiki, row.rev_id, row.page_id, is_deleted=True)
            else:
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
        df['language'] = active_wikis.language_name[active_wikis.dbname ==
                                                    wiki].to_string(index=False)
        df['is_reverted'] = None

        api_session = mwapi.Session(active_wikis.url[active_wikis.dbname == wiki].to_string(
            index=False), user_agent="Revert detection <cxie@wikimedia.org>")
        timeout = 10  # kill check_reverted_db if it runs more than 10 seconds

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

all_edits.to_csv(
    'data/baseline_ios/all_edits.tsv',
    sep='\t',
    index=False,
    quoting=csv.QUOTE_NONE)

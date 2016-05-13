#!/usr/bin/env python

"""'rethinkdb index-rebuild' recreates outdated secondary indexes in a cluster.
  This should be used after upgrading to a newer version of rethinkdb.  There
  will be a notification in the web UI if any secondary indexes are out-of-date."""

from __future__ import print_function

import os, random, sys, time, traceback
from . import utils_common, net
r = utils_common.r

usage = "rethinkdb index-rebuild [-c HOST:PORT] [-n NUM] [-r (DB | DB.TABLE)] [--tls-cert FILENAME] [-p] [--password-file FILENAME]..."
help_epilog = '''
FILE: the archive file to restore data from

EXAMPLES:
rethinkdb index-rebuild -c mnemosyne:39500
  rebuild all outdated secondary indexes from the cluster through the host 'mnemosyne',
  one at a time

rethinkdb index-rebuild -r test -r production.users -n 5
  rebuild all outdated secondary indexes from a local cluster on all tables in the
  'test' database as well as the 'production.users' table, five at a time
'''

# Prefix used for indexes that are being rebuilt
temp_index_prefix = '$reql_temp_index$_'

def parse_options(argv):
    parser = utils_common.CommonOptionsParser(usage=usage, epilog=help_epilog)

    parser.add_option("-r", "--rebuild", dest="db_table",   metavar="DB|DB.TABLE", default=[],    help="databases or tables to rebuild indexes on (default: all, may be specified multiple times)",  action="append", type="db_table")
    parser.add_option("-n",              dest="concurrent", metavar="NUM",         default=1,     help="concurrent indexes to rebuild (default: 1)", type="pos_int")
    parser.add_option("--force",         dest="force",      action="store_true",   default=False, help="rebuild non-outdated indexes")

    options, args = parser.parse_args(argv)

    # Check validity of arguments
    if len(args) != 0:
        parser.error("Error: No positional arguments supported. Unrecognized option '%s'" % args[0])

    return options

def get_indexes(progress, db_tables, only_outdated=False):
    conn = utils_common.getConnection()
    res = []
    if len(db_tables) == 0:
        dbs = r.db_list().run(conn)
        db_tables = [utils_common.DbTable(db, None) for db in dbs]

    for db, table in db_tables:
        table_list = (table,) if table else r.db(db).table_list().run(conn)
        for table in table_list:
            indexes = None
            if only_outdated:
                indexes = r.db(db).table(table).index_status().filter({'outdated':True}).get_field('index').run(conn)
            else:
                indexes = r.db(db).table(table).index_status().get_field('index').run(conn)
            for index in indexes:
                res.append({'db':db, 'table':table, 'name':index})
    return res

def drop_outdated_temp_indexes(progress, indexes):
    conn = utils_common.getConnection()
    indexes_to_drop = [i for i in indexes if i['name'].find(temp_index_prefix) == 0]
    for index in indexes_to_drop:
        r.db(index['db']).table(index['table']).index_drop(index['name']).run(conn)
        indexes.remove(index)

def create_temp_index(progress, index):
    conn = utils_common.getConnection()
    # If this index is already being rebuilt, don't try to recreate it
    extant_indexes = r.db(index['db']).table(index['table']).index_status().map(lambda i: i['index']).run(conn)
    if index['temp_name'] not in extant_indexes:
        index_fn = r.db(index['db']).table(index['table']).index_status(index['name']).nth(0)['function']
        r.db(index['db']).table(index['table']).index_create(index['temp_name'], index_fn).run(conn)

def get_index_progress(progress, index):
    conn = utils_common.getConnection()
    status = r.db(index['db']).table(index['table']).index_status(index['temp_name']).nth(0).run(conn)
    index['function'] = status['function']
    if status['ready']:
        return None
    else:
        return float(status.get('progress'))

def rename_index(progress, index):
    conn = utils_common.getConnection()
    r.db(index['db']).table(index['table']).index_rename(index['temp_name'], index['name'], overwrite=True).run(conn)

def check_index_renamed(progress, index):
    conn = utils_common.getConnection()
    status = r.db(index['db']).table(index['table']).index_status(index['name']).nth(0).run(conn)
    if status['outdated'] or status['ready'] != True or status['function'] != index['function']:
        raise RuntimeError("Error: failed to rename `%(db)s.%(table)s` temporary index for `%(name)s`" % index)


def rebuild_indexes(options):
    ISSUE2904FORMAT = r"ReQL error during 'create `%(db)s.%(table)s` index `%(name)s`': Index `%(temp_name)s` already exists on table `%(db)s.%(table)s`."
    
    indexes_to_build = utils_common.rdb_call_wrapper("get indexes", get_indexes, options.db_table, only_outdated=(not options.force))
    indexes_in_progress = []
    
    # Drop any outdated indexes with the temp_index_prefix
    utils_common.rdb_call_wrapper("drop temporary outdated indexes", drop_outdated_temp_indexes, indexes_to_build)

    random.shuffle(indexes_to_build)
    total_indexes = len(indexes_to_build)
    indexes_completed = 0
    progress_ratio = 0.0
    highest_progress = 0.0
    
    print("Rebuilding %d index%s: %s" % (total_indexes, 'es' if total_indexes > 1 else '',  ", ".join(["`%(db)s.%(table)s:%(name)s`" % i for i in indexes_to_build])))

    while len(indexes_to_build) > 0 or len(indexes_in_progress) > 0:
        # Make sure we're running the right number of concurrent index rebuilds
        while len(indexes_to_build) > 0 and len(indexes_in_progress) < options.concurrent:
            index = indexes_to_build.pop()
            indexes_in_progress.append(index)
            index['temp_name'] = temp_index_prefix + index['name']
            index['progress'] = 0
            index['ready'] = False

            try:
                utils_common.rdb_call_wrapper("create `%(db)s.%(table)s` index `%(name)s`" % index, create_temp_index, index)
            except RuntimeError as ex:
                # This may be caused by a spurious failure (see github issue #2904), ignore if so
                if ex.message != ISSUE2904FORMAT % index:
                    raise

        # Report progress
        highest_progress = max(highest_progress, progress_ratio)
        utils_common.print_progress(highest_progress)

        # Check the status of indexes in progress
        progress_ratio = 0.0
        for index in indexes_in_progress:
            index_progress = utils_common.rdb_call_wrapper("progress `%(db)s.%(table)s` index `%(name)s`" % index, get_index_progress, index)
            if index_progress is None:
                index['ready'] = True
                try:
                    utils_common.rdb_call_wrapper("rename `%(db)s.%(table)s` index `%(name)s`" % index, rename_index, index)
                except r.ReqlRuntimeError as ex:
                    # This may be caused by a spurious failure (see github issue #2904), check if it actually succeeded
                    if ex.message != ISSUE2904FORMAT % index:
                        raise
                    utils_common.rdb_call_wrapper("check rename `%(db)s.%(table)s` index `%(name)s`" % index, check_index_renamed, index)
            else:
                progress_ratio += index_progress / total_indexes

        indexes_in_progress = [index for index in indexes_in_progress if not index['ready']]
        indexes_completed = total_indexes - len(indexes_to_build) - len(indexes_in_progress)
        progress_ratio += float(indexes_completed) / total_indexes

        if len(indexes_in_progress) == options.concurrent or \
           (len(indexes_in_progress) > 0 and len(indexes_to_build) == 0):
            # Short sleep to keep from killing the CPU
            time.sleep(0.1)

    # Make sure the progress bar says we're done and get past the progress bar line
    utils_common.print_progress(1.0)
    print("")

def main(argv=None):
    options = parse_options(argv or sys.argv[2:])
    start_time = time.time()
    try:
        rebuild_indexes(options)
    except Exception as ex:
        if options.debug:
            traceback.print_exc()
        print(ex, file=sys.stderr)
        return 1
    print("Done (%d seconds)" % (time.time() - start_time))
    return 0

if __name__ == "__main__":
    sys.exit(main())

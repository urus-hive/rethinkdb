#!/usr/bin/env python

from __future__ import print_function

import csv, ctypes, datetime, json, multiprocessing, numbers, optparse, tempfile
import os, re, signal, sys, time, traceback

from . import utils_common, net
r = utils_common.r

# When running a subprocess, we may inherit the signal handler - remove it
signal.signal(signal.SIGINT, signal.SIG_DFL)

try:
    unicode
except NameError:
    unicode = str
try:
    from multiprocessing import SimpleQueue
except ImportError:
    from multiprocessing.queues import SimpleQueue

usage = """rethinkdb export [-c HOST:PORT] [-p] [--password-file FILENAME] [--tls-cert filename] [-d DIR] [-e (DB | DB.TABLE)]...
      [--format (csv | json | ndjson)] [--fields FIELD,FIELD...] [--delimiter CHARACTER]
      [--clients NUM]"""
help_description = '`rethinkdb export` exports data from a RethinkDB cluster into a directory'
help_epilog = '''
EXAMPLES:
rethinkdb export -c mnemosyne:39500
  Export all data from a cluster running on host 'mnemosyne' with a client port at 39500.

rethinkdb export -e test -d rdb_export
  Export only the 'test' database on a local cluster into a named directory.

rethinkdb export -c hades -e test.subscribers -p
  Export a specific table from a cluster running on host 'hades' which requires a password.

rethinkdb export --format csv -e test.history --fields time,message --delimiter ';'
  Export a specific table from a local cluster in CSV format with the fields 'time' and 'message',
  using a semicolon as field delimiter (rather than a comma).

rethinkdb export --fields id,value -e test.data
  Export a specific table from a local cluster in JSON format with only the fields 'id' and 'value'.
'''

def parse_options(argv, prog=None):
    defaultDir = os.path.realpath("./rethinkdb_export_%s" % datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")) # "
    
    parser = utils_common.CommonOptionsParser(usage=usage, description=help_description, epilog=help_epilog, prog=prog)
    
    parser.add_option("-d", "--directory", dest="directory", metavar="DIRECTORY",       default=defaultDir, help='directory to output to (default: rethinkdb_export_DATE_TIME)', type="new_file")
    parser.add_option("-e", "--export",    dest="db_tables", metavar="DB|DB.TABLE",     default=[],         help='limit dump to the given database or table (may be specified multiple times)', action="append", type="db_table")
    parser.add_option("--fields",          dest="fields",    metavar="<FIELD>,...",     default=None,       help='export only specified fields (required for CSV format)')
    parser.add_option("--format",          dest="format",    metavar="json|csv|ndjson", default="json",     help='format to write (defaults to json. ndjson is newline delimited json.)', type="choice", choices=['json', 'csv', 'ndjson'])
    parser.add_option("--clients",         dest="clients",   metavar="NUM",             default=3,          help='number of tables to export simultaneously (default: 3)', type="pos_int")
    
    csvGroup = optparse.OptionGroup(parser, 'CSV options')
    csvGroup.add_option("--delimiter", dest="delimiter",     metavar="CHARACTER",       default=None,       help="character to be used as field delimiter, or '\\t' for tab (default: ',')")
    parser.add_option_group(csvGroup)
    
    options, args = parser.parse_args(argv)
    
    # -- Check validity of arguments
    
    if len(args) != 0:
        raise parser.error("No positional arguments supported. Unrecognized option(s): %s" % args)
    
    if options.fields:
        if len(options.db_tables) != 1 or options.db_tables[0].table is None:
            parser.error("The --fields option can only be used when exporting a single table")
        options.fields = options.fields.split(",")
    
    # - format specific validation
    
    if options.format == "csv":
        if options.fields is None:
            parser.error("CSV files require the '--fields' option to be specified.")
        
        if options.delimiter is None: 
            options.delimiter = ","
        elif options.delimiter == "\\t":
            options.delimiter = "\t"
        elif len(options.delimiter) != 1:
            parser.error("Specify exactly one character for the --delimiter option: %s" % options.delimiter)
    else:
        if options.delimiter:
            parser.error("--delimiter option is only valid for CSV file formats")
    
    # -
    
    return options

# This is called through rdb_call_wrapper and may be called multiple times if
# connection errors occur.  Don't bother setting progress, because this is a
# fairly small operation.
def get_tables(progress, tables):
    conn = utils_common.getConnection()
    dbs = r.db_list().filter(r.row.ne('rethinkdb')).run(conn)
    res = []

    if len(tables) == 0:
        tables = [(db, None) for db in dbs]

    for db_table in tables:
        if db_table[0] == 'rethinkdb':
            raise RuntimeError("Error: Cannot export tables from the system database: 'rethinkdb'")
        if db_table[0] not in dbs:
            raise RuntimeError("Error: Database '%s' not found" % db_table[0])

        if db_table[1] is None: # This is just a db name
            res.extend([(db_table[0], table) for table in r.db(db_table[0]).table_list().run(conn)])
        else: # This is db and table name
            if db_table[1] not in r.db(db_table[0]).table_list().run(conn):
                raise RuntimeError("Error: Table not found: '%s.%s'" % tuple(db_table))
            res.append(tuple(db_table))

    # Remove duplicates by making results a set
    return set(res)

# This is called through rdb_call_wrapper and may be called multiple times if
# connection errors occur.  Don't bother setting progress, because we either
# succeed or fail, there is no partial success.
def write_table_metadata(progress, db, table, base_path):
    conn = utils_common.getConnection()
    table_info = r.db(db).table(table).info().run(conn)

    # Rather than just the index names, store all index information
    table_info['indexes'] = r.db(db).table(table).index_status().run(conn, binary_format='raw')

    out = open(base_path + "/%s/%s.info" % (db, table), "w")
    out.write(json.dumps(table_info) + "\n")
    out.close()
    return table_info

# This is called through rdb_call_wrapper and may be called multiple times if
# connection errors occur.  In order to facilitate this, we do an order_by by the
# primary key so that we only ever get a given row once.
def read_table_into_queue(progress, db, table, pkey, task_queue, progress_info, exit_event):
    conn = utils_common.getConnection()
    read_rows = 0
    if progress[0] is None:
        cursor = r.db(db).table(table).order_by(index=pkey).run(conn, time_format="raw", binary_format='raw')
    else:
        cursor = r.db(db).table(table).between(progress[0], None, left_bound="open").order_by(index=pkey).run(conn, time_format="raw", binary_format='raw')

    try:
        for row in cursor:
            if exit_event.is_set():
                break
            task_queue.put([row])

            # Set progress so we can continue from this point if a connection error occurs
            progress[0] = row[pkey]

            # Update the progress every 20 rows - to reduce locking overhead
            read_rows += 1
            if read_rows % 20 == 0:
                progress_info[0].value += 20
    finally:
        progress_info[0].value += read_rows % 20

    # Export is done - since we used estimates earlier, update the actual table size
    progress_info[1].value = progress_info[0].value

def json_writer(filename, fields, task_queue, error_queue, format):
    try:
        with open(filename, "w") as out:
            first = True
            if format != "ndjson":
                out.write("[")
            item = task_queue.get()
            while not isinstance(item, StopIteration):
                row = item[0]
                if fields is not None:
                    for item in list(row.keys()):
                        if item not in fields:
                            del row[item]
                if first:
                    if format == "ndjson":
                        out.write(json.dumps(row))
                    else:
                        out.write("\n" + json.dumps(row))
                    first = False
                elif format == "ndjson":
                    out.write("\n" + json.dumps(row))
                else:
                    out.write(",\n" + json.dumps(row))

                item = task_queue.get()
            if format != "ndjson":
                out.write("\n]\n")
    except:
        ex_type, ex_class, tb = sys.exc_info()
        error_queue.put((ex_type, ex_class, traceback.extract_tb(tb)))

        # Read until the exit task so the readers do not hang on pushing onto the queue
        while not isinstance(task_queue.get(), StopIteration):
            pass

def csv_writer(filename, fields, delimiter, task_queue, error_queue):
    try:
        with open(filename, "w") as out:
            out_writer = csv.writer(out, delimiter=delimiter)
            out_writer.writerow(fields)

            item = task_queue.get()
            while not isinstance(item, StopIteration):
                row = item[0]
                info = []
                # If the data is a simple type, just write it directly, otherwise, write it as json
                for field in fields:
                    if field not in row:
                        info.append(None)
                    elif isinstance(row[field], numbers.Number):
                        info.append(str(row[field]))
                    elif isinstance(row[field], str):
                        info.append(row[field])
                    elif isinstance(row[field], unicode):
                        info.append(row[field].encode('utf-8'))
                    else:
                        info.append(json.dumps(row[field]))
                out_writer.writerow(info)
                item = task_queue.get()
    except:
        ex_type, ex_class, tb = sys.exc_info()
        error_queue.put((ex_type, ex_class, traceback.extract_tb(tb)))

        # Read until the exit task so the readers do not hang on pushing onto the queue
        while not isinstance(task_queue.get(), StopIteration):
            pass

def get_all_table_sizes(db_table_set):
    def get_table_size(progress, db, table):
        conn = utils_common.getConnection()
        return r.db(db).table(table).info()['doc_count_estimates'].sum().run(conn)

    ret = dict()
    for pair in db_table_set:
        db, table = pair
        ret[pair] = int(utils_common.rdb_call_wrapper("count", get_table_size, db, table))

    return ret

def export_table(db, table, directory, fields, delimiter, format, error_queue, progress_info, sindex_counter, exit_event):
    writer = None

    try:
        table_info = utils_common.rdb_call_wrapper("info", write_table_metadata, db, table, directory)
        sindex_counter.value += len(table_info["indexes"])
        
        # -- start the writer
        
        task_queue = SimpleQueue()
        writer = None
        if format == "json":
            filename = directory + "/%s/%s.json" % (db, table)
            writer = multiprocessing.Process(target=json_writer,
                                           args=(filename, fields, task_queue, error_queue, format))
        elif format == "csv":
            filename = directory + "/%s/%s.csv" % (db, table)
            writer = multiprocessing.Process(target=csv_writer,
                                           args=(filename, fields, delimiter, task_queue, error_queue))
        elif format == "ndjson":
            filename = directory + "/%s/%s.ndjson" % (db, table)
            writer = multiprocessing.Process(target=json_writer,
                                           args=(filename, fields, task_queue, error_queue, format))
        else:
            raise RuntimeError("unknown format type: %s" % format)
        writer.start()
        
        # -- read in the data source
        
        utils_common.rdb_call_wrapper("table scan", read_table_into_queue, db, table,
                         table_info["primary_key"], task_queue, progress_info, exit_event)
    except (r.ReqlError, r.ReqlDriverError) as ex:
        error_queue.put((RuntimeError, RuntimeError(ex.message), traceback.extract_tb(sys.exc_info()[2])))
    except:
        ex_type, ex_class, tb = sys.exc_info()
        error_queue.put((ex_type, ex_class, traceback.extract_tb(tb)))
    finally:
        if writer is not None and writer.is_alive():
            task_queue.put(StopIteration())
            writer.join()

def abort_export(signum, frame, exit_event, interrupt_event):
    interrupt_event.set()
    exit_event.set()

# We sum up the row count from all tables for total percentage completion
#  This is because table exports can be staggered when there are not enough clients
#  to export all of them at once.  As a result, the progress bar will not necessarily
#  move at the same rate for different tables.
def update_progress(progress_info, options):
    rows_done = 0
    total_rows = 1
    for current, max_count in progress_info:
        curr_val = current.value
        max_val = max_count.value
        if curr_val < 0:
            # There is a table that hasn't finished counting yet, we can't report progress
            rows_done = 0
            break
        else:
            rows_done += curr_val
            total_rows += max_val

    if not options.quiet:
        utils_common.print_progress(float(rows_done) / total_rows, padding=4)

def run_clients(options, partialDirectory, db_table_set):
    # Spawn one client for each db.table
    exit_event = multiprocessing.Event()
    processes = []
    error_queue = SimpleQueue()
    interrupt_event = multiprocessing.Event()
    sindex_counter = multiprocessing.Value(ctypes.c_longlong, 0)

    signal.signal(signal.SIGINT, lambda a, b: abort_export(a, b, exit_event, interrupt_event))
    errors = [ ]

    try:
        sizes = get_all_table_sizes(db_table_set)

        progress_info = []

        arg_lists = []
        for db, table in db_table_set:
            progress_info.append((multiprocessing.Value(ctypes.c_longlong, 0),
                                  multiprocessing.Value(ctypes.c_longlong, sizes[(db, table)])))
            arg_lists.append((db, table,
                              partialDirectory,
                              options.fields,
                              options.delimiter,
                              options.format,
                              error_queue,
                              progress_info[-1],
                              sindex_counter,
                              exit_event,
                              ))


        # Wait for all tables to finish
        while processes or arg_lists:
            time.sleep(0.1)

            while not error_queue.empty():
                exit_event.set() # Stop immediately if an error occurs
                errors.append(error_queue.get())

            processes = [process for process in processes if process.is_alive()]

            if len(processes) < options.clients and len(arg_lists) > 0:
                newProcess = multiprocessing.Process(target=export_table, args=arg_lists.pop(0))
                newProcess.start()
                processes.append(newProcess)

            update_progress(progress_info, options)

        # If we were successful, make sure 100% progress is reported
        # (rows could have been deleted which would result in being done at less than 100%)
        if len(errors) == 0 and not interrupt_event.is_set() and not options.quiet:
            utils_common.print_progress(1.0, padding=4)

        # Continue past the progress output line and print total rows processed
        def plural(num, text, plural_text):
            return "%d %s" % (num, text if num == 1 else plural_text)

        if not options.quiet:
            print("\n    %s exported from %s, with %s" %
                  (plural(sum([max(0, info[0].value) for info in progress_info]), "row", "rows"),
                   plural(len(db_table_set), "table", "tables"),
                   plural(sindex_counter.value, "secondary index", "secondary indexes")))
    finally:
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    if interrupt_event.is_set():
        raise RuntimeError("Interrupted")

    if len(errors) != 0:
        # multiprocessing queues don't handling tracebacks, so they've already been stringified in the queue
        for error in errors:
            print("%s" % error[1], file=sys.stderr)
            if options.debug:
                print("%s traceback: %s" % (error[0].__name__, error[2]), file=sys.stderr)
        raise RuntimeError("Errors occurred during export")

def run(options):
    # Make sure this isn't a pre-`reql_admin` cluster - which could result in data loss
    # if the user has a database named 'rethinkdb'
    utils_common.rdb_call_wrapper("version check", utils_common.check_minimum_version, (1, 16, 0))
    
    # get the complete list of tables
    db_table_set = utils_common.rdb_call_wrapper("table list", get_tables, options.db_tables)

    # Determine the actual number of client processes we'll have
    options.clients = min(options.clients, len(db_table_set))
    
    # create the working directory and its structure
    parentDir = os.path.dirname(options.directory)
    if not os.path.exists(parentDir):
        if os.path.isdir(parentDir):
            raise RuntimeError("Ouput parent directory is not a directory: %s" % (opt, value))
        try:
            os.makedirs(parentDir)
        except OSError as e:
            raise optparse.OptionValueError("Unable to create parent directory for %s: %s (%s)" % (opt, value, e.strerror))
    workingDir = tempfile.mkdtemp(prefix=os.path.basename(options.directory) + '_partial_', dir=os.path.dirname(options.directory))
    try:
        for db in set([db for db, table in db_table_set]):
            os.makedirs(os.path.join(workingDir, str(db)))
    except OSError as e:
        raise RuntimeError("Failed to create temporary directory (%s): %s" % (e.filename, ex.strerror))
    
    # Run the export
    run_clients(options, workingDir, db_table_set)
    
    # Move the temporary directory structure over to the original output directory
    try:
        os.rename(workingDir, options.directory)
    except OSError as e:
        raise RuntimeError("Failed to move temporary directory to output directory (%s): %s" % (options.directory, ex.strerror))

def main(argv=None, prog=None):
    options = parse_options(argv or sys.argv[2:], prog=prog)
    
    start_time = time.time()
    try:
        run(options)
    except RuntimeError as ex:
        print(ex, file=sys.stderr)
        return 1
    if not options.quiet:
        print("  Done (%d seconds)" % (time.time() - start_time))
    return 0

if __name__ == "__main__":
    sys.exit(main())

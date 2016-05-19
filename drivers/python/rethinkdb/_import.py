#!/usr/bin/env python

'''`rethinkdb import` loads data into a RethinkDB cluster'''

from __future__ import print_function

import codecs, collections, csv, ctypes, datetime, json, multiprocessing
import optparse, os, re, signal, sys, threading, time, traceback

try:
    import Queue
except NameError:
    import queue as Queue

from . import utils_common, net
r = utils_common.r

# Used because of API differences in the csv module, taken from
# http://python3porting.com/problems.html
PY3 = sys.version > "3"

#json parameters
json_read_chunk_size = 32 * 1024
json_max_buffer_size = 128 * 1024 * 1024
max_nesting_depth = 100
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    from itertools import imap
except ImportError:
    imap = map
try:
    xrange
except NameError:
    xrange = range
try:
    from multiprocessing import SimpleQueue
except ImportError:
    from multiprocessing.queues import SimpleQueue

Error = collections.namedtuple("Error", ["message", "traceback", "file"])
SourceFile = collections.namedtuple("SourceFile", ['path', 'db', 'table', 'format', 'primary_key', 'indexes'])

usage = """rethinkdb import -d DIR [-c HOST:PORT] [--tls-cert FILENAME] [-p] [--password-file FILENAME]
      [--force] [-i (DB | DB.TABLE)] [--clients NUM]
      [--shards NUM_SHARDS] [--replicas NUM_REPLICAS]
  rethinkdb import -f FILE --table DB.TABLE [-c HOST:PORT] [--tls-cert FILENAME] [-p] [--password-file FILENAME]
      [--force] [--clients NUM] [--format (csv | json)] [--pkey PRIMARY_KEY]
      [--shards NUM_SHARDS] [--replicas NUM_REPLICAS]
      [--delimiter CHARACTER] [--custom-header FIELD,FIELD... [--no-header]]"""
help_epilog = '''
EXAMPLES:

rethinkdb import -d rdb_export -c mnemosyne:39500 --clients 128
  Import data into a cluster running on host 'mnemosyne' with a client port at 39500,
  using 128 client connections and the named export directory.

rethinkdb import -f site_history.csv --format csv --table test.history --pkey count
  Import data into a local cluster and the table 'history' in the 'test' database,
  using the named CSV file, and using the 'count' field as the primary key.

rethinkdb import -d rdb_export -c hades -p -i test
  Import data into a cluster running on host 'hades' which requires a password,
  using only the database 'test' from the named export directory.

rethinkdb import -f subscriber_info.json --fields id,name,hashtag --force
  Import data into a local cluster using the named JSON file, and only the fields
  'id', 'name', and 'hashtag', overwriting any existing rows with the same primary key.

rethinkdb import -f user_data.csv --delimiter ';' --no-header --custom-header id,name,number
  Import data into a local cluster using the named CSV file with no header and instead
  use the fields 'id', 'name', and 'number', the delimiter is a semicolon (rather than
  a comma).
'''

def parse_options(argv, prog=None):
    parser = utils_common.CommonOptionsParser(usage=usage, epilog=help_epilog, prog=prog)
    
    parser.add_option("--clients",         dest="clients",    metavar="CLIENTS",    default=8,      help="client connections to use (default: 8)", type="pos_int")
    parser.add_option("--hard-durability", dest="durability", action="store_const", default="soft", help="use hard durability writes (slower, uses less memory)", const="hard")
    parser.add_option("--force",           dest="force",      action="store_true",  default=False,  help="import even if a table already exists, overwriting duplicate primary keys")
    
    # Replication settings
    replicationOptionsGroup = optparse.OptionGroup(parser, "Replication Options")
    replicationOptionsGroup.add_option("--shards",   dest="create_args", metavar="SHARDS",   help="shards to setup on created tables (default: 1)",   type="pos_int", action="add_key")
    replicationOptionsGroup.add_option("--replicas", dest="create_args", metavar="REPLICAS", help="replicas to setup on created tables (default: 1)", type="pos_int", action="add_key")
    parser.add_option_group(replicationOptionsGroup)

    # Directory import options
    dirImportGroup = optparse.OptionGroup(parser, "Directory Import Options")
    dirImportGroup.add_option("-d", "--directory",      dest="directory", metavar="DIRECTORY",   default=None, help="directory to import data from")
    dirImportGroup.add_option("-i", "--import",         dest="db_tables", metavar="DB|DB.TABLE", default=[],   help="restore only the given database or table (may be specified multiple times)", action="append", type="db_table")
    dirImportGroup.add_option("--no-secondary-indexes", dest="sindexes",  action="store_false",  default=True, help="do not create secondary indexes")
    parser.add_option_group(dirImportGroup)

    # File import options
    fileImportGroup = optparse.OptionGroup(parser, "File Import Options")
    fileImportGroup.add_option("-f", "--file", dest="file",         metavar="FILE",        default=None, help="file to import data from", type="file")
    fileImportGroup.add_option("--table",      dest="import_table", metavar="DB.TABLE",    default=None, help="table to import the data into")
    fileImportGroup.add_option("--fields",     dest="fields",       metavar="FIELD,...",   default=None, help="limit which fields to use when importing one table")
    fileImportGroup.add_option("--format",     dest="format",       metavar="json|csv",    default=None, help="format of the file (default: json, accepts newline delimited json)", type="choice", choices=["json", "csv"])
    fileImportGroup.add_option("--pkey",       dest="create_args",  metavar="PRIMARY_KEY", default=None, help="field to use as the primary key in the table", action="add_key")
    parser.add_option_group(fileImportGroup)
    
    # CSV import options
    csvImportGroup = optparse.OptionGroup(parser, "CSV Options")
    csvImportGroup.add_option("--delimiter",     dest="delimiter",     metavar="CHARACTER", default=None, help="character separating fields, or '\\t' for tab")
    csvImportGroup.add_option("--no-header",     dest="no_header",     action="store_true", default=None, help="do not read in a header of field names")
    csvImportGroup.add_option("--custom-header", dest="custom_header", metavar="FIELD,...", default=None, help="header to use (overriding file header), must be specified if --no-header")
    parser.add_option_group(csvImportGroup)
    
    # JSON import options
    jsonOptionsGroup = optparse.OptionGroup(parser, "JSON Options")
    jsonOptionsGroup.add_option("--max-document-size", dest="max_document_size", metavar="MAX_SIZE",  default=0, help="maximum allowed size (bytes) for a single JSON document (default: 128MiB)", type="pos_int")
    jsonOptionsGroup.add_option("--max-nesting-depth", dest="max_nesting_depth", metavar="MAX_DEPTH", default=0, help="maximum depth of the JSON documents (default: 100)", type="pos_int")
    parser.add_option_group(jsonOptionsGroup)
    
    options, args = parser.parse_args(argv)

    # Check validity of arguments

    if len(args) != 0:
        raise parser.error("No positional arguments supported. Unrecognized option(s): %s" % args)
    
    # - create_args
    if options.create_args is None:
        options.create_args = {}
    
    # - options based on file/directory import
    
    if options.directory and options.file:
        parser.error("-f/--file and -d/--directory can not be used together")
    
    elif options.directory:
        if not os.path.exists(options.directory):
            parser.error("-d/--directory does not exist: %s" % options.directory)
        if not os.path.isdir(options.directory):
            parser.error("-d/--directory is not a directory: %s" % options.directory)
        options.directory = os.path.realpath(options.directory)
        
        # disallow invalid options
        if options.import_table:
            parser.error("--table option is not valid when importing a directory")
        if options.fields:
            parser.error("--fields option is not valid when importing a directory")
        if options.format:
            parser.error("--format option is not valid when importing a directory")
        if options.create_args:
            parser.error("--pkey option is not valid when importing a directory")
        
        if options.delimiter:
            parser.error("--delimiter option is not valid when importing a directory")
        if options.no_header:
            parser.error("--no-header option is not valid when importing a directory")
        if options.custom_header:
            parser.error("table create options are not valid when importing a directory: %s" % ", ".join([x.lower().replace("_", " ") for x in options.custom_header.keys()]))
        
        # check valid options
        if not os.path.isdir(options.directory):
            parser.error("Directory to import does not exist: %s" % options.directory)
        
        if options.fields and (len(options.db_tables) > 1 or options.db_tables[0].table is None):
            parser.error("--fields option can only be used when importing a single table")
        
    elif options.file:
        if not os.path.exists(options.file):
            parser.error("-f/--file does not exist: %s" % options.file)
        if not os.path.isfile(options.file):
            parser.error("-f/--file is not a file: %s" % options.file)
        options.file = os.path.realpath(options.file)
        
        # format
        if options.format is None:
            _, options.format = os.path.splitext(options.file)
        
        # import_table
        if options.import_table:
            res = utils_common._tableNameRegex.match(options.import_table)
            if res and res.group("table"):
                options.import_table = utils_common.DbTable(res.group("db"), res.group("table"))
            else:
                parser.error("Invalid --table option: %s" )
        
        # fields
        options.fields = options.fields.split(",") if options.fields else None
        
        if options.format == "csv":
            # disallow invalid options
            if options.db_tables:
                parser.error("-i/--import can only be used when importing a directory")
            if options.sindexes:
                parser.error("--no-secondary-indexes can only be used when importing a directory")
            
            if options.max_document_size:
                parser.error("--max_document_size only affects importing JSON documents")
            
            # required options
            if not options.table:
                paser.error("A value is required for --table when importing from a file")
            
            # delimiter
            if options.delimiter is None: 
                options.delimiter = ","
            elif options.delimiter == "\\t":
                options.delimiter = "\t"
            elif len(options.delimiter) != 1:
                parser.error("Specify exactly one character for the --delimiter option: %s" % options.delimiter)
            
            # no_header
            if options.no_header is None:
                options.no_header = False
            elif options.custom_header is None:
                parser.error("--custom-header is required if --no-header is specified")
            
            # custom_header
            if options.custom_header:
                options.custom_header = options.custom_header.split(",")
                
        elif options.format == "json": # json format
            # disallow invalid options
            if options.db_tables:
                parser.error("-i/--import can only be used when importing a directory")
            if options.sindexes:
                parser.error("--no-secondary-indexes can only be used when importing a directory")
            
            if options.delimiter is not None:
                parser.error("--delimiter option is not valid for json files")
            if options.no_header is not False:
                parser.error("--no-header option is not valid for json files")
            if options.custom_header is not None:
                parser.error("--custom-header option is not valid for json files")
            
            # default options
            options.format = "json"
            
            if options.max_document_size > 0:
                global json_max_buffer_size
                json_max_buffer_size=options.max_document_size
            
            options.file = os.path.abspath(options.file)
        
        else:
            parser.error("Unrecognized file format: %s" % options.format)
        
    else:
        parser.error("Either -f/--file or -d/--directory is required")
    
    # --
    
    # max_nesting_depth
    if options.max_nesting_depth > 0:
        global max_nesting_depth
        max_nesting_depth = options.max_nesting_depth
    
    # --
    
    return options

# This is run for each client requested, and accepts tasks from the reader processes
def table_writer(file_info, options, task_queue, error_queue, rows_written, batch_timeout=1, batch_max=200):
    try:
        conflict_action = "replace" if options.force else "error"
        tbl = r.db(file_info.db).table(file_info.table)
        
        shouldContinue = True
        while shouldContinue:
            write_count = 0
            
            # get a batch
            deadline = time.time() + batch_timeout
            batch = []
            while len(batch) < batch_max:
                waitTime = deadline - time.time()
                if waitTime <= 0:
                    break
                try:
                    item = task_queue.get(timeout=waitTime)
                    if isinstance(item, StopIteration):
                        shouldContinue = False
                        break
                    else:
                        # block bad items
                        if not isinstance(item, dict):
                            error_queue.put(Error('Error importing row on table %s.%s:\n%s' % (file_info.db, file_info.table, str(item)), None, file_info.path))
                            continue
                        
                        # apply the fields filter
                        if options.fields:
                            for key in [x for x in item.keys() if x not in options.fields and x != file_info.primary_key]:
                                del item[key]
                        
                        batch.append(item)
                except Queue.Empty:
                    break
            
            # write the batch to the database
            try:
                res = utils_common.retryQuery(
                    "write batch to %s.%s" % (file_info.db, file_info.table),
                    tbl.insert(r.expr(batch, nesting_depth=max_nesting_depth), durability=options.durability, conflict=conflict_action)
                )
                
                if res["errors"] > 0:
                    raise RuntimeError("Error when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                modified = res["inserted"] + res["replaced"] + res["unchanged"]
                if modified != len(batch):
                    raise RuntimeError("The inserted/replaced/unchanged number did not match when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                
                write_count += res["inserted"] + res["replaced"] + res["unchanged"]
                
            except r.ReqlError:
                # the error might have been caused by a comm or temporary error causing a partial batch write
                
                for row in batch:
                    if not file_info.primary_key in row:
                        raise RuntimeError("Connection error while importing.  Current row does not have the specified primary key (%s), so cannot guarantee absence of duplicates" % file_info.primary_key)
                    res = None
                    if conflict_action == "replace":
                        res = utils_common.retryQuery(
                            "write row to %s.%s" % (file_info.db, file_info.table),
                            tbl.insert(r.expr(row, nesting_depth=max_nesting_depth), durability=durability, conflict=conflict_action)
                        )
                    else:
                        existingRow = utils_common.retryQuery(
                            "read row from %s.%s" % (file_info.db, file_info.table),
                            tbl.get(row[file_info.primary_key])
                        )
                        if not existingRow:
                            res = utils_common.retryQuery(
                                "write row to %s.%s" % (file_info.db, file_info.table),
                                tbl.insert(r.expr(row, nesting_depth=max_nesting_depth), durability=durability, conflict=conflict_action)
                            )
                        elif existingRow != row:
                            raise RuntimeError("Duplicate primary key `%s`:\n%s\n%s" % (file_info.primary_key, str(row), str(existingRow)))
                    
                    if res["errors"] > 0:
                        raise RuntimeError("Error when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                    if res["inserted"] + res["replaced"] + res["unchanged"] != 1:
                        raise RuntimeError("The inserted/replaced/unchanged number was not 1 when inserting on '%s.%s': %s" % (db, table, res))
                    write_count += 1
            
            with rows_written.get_lock():
                rows_written.value += write_count
    except Exception as e:
        error_queue.put(Error(str(e), traceback.format_exc, file_info.path))

batch_length_limit = 200
batch_size_limit = 500000

class InterruptedError(Exception):
    def __str__(self):
        return "Interrupted"

# This function is called for each object read from a file by the reader processes
#  and will push tasks to the client processes on the task queue
def object_callback(obj, db, table, task_queue, object_buffers, buffer_sizes, fields, exit_event):
    
    if exit_event.is_set():
        raise InterruptedError()

    if not isinstance(obj, dict):
        raise RuntimeError("Error: Invalid input, expected an object, but got %s" % type(obj))

    # filter out fields
    if fields is not None:
        for key in list(obj.keys()):
            if key not in fields:
                del obj[key]

    # Pickle the object here because we want an accurate size, and it'll pickle anyway for IPC
    object_buffers.append(pickle.dumps(obj))
    buffer_sizes.append(len(object_buffers[-1]))
    if len(object_buffers) >= batch_length_limit or sum(buffer_sizes) > batch_size_limit:
        task_queue.put((db, table, object_buffers))
        del object_buffers[0:len(object_buffers)]
        del buffer_sizes[0:len(buffer_sizes)]
    return obj

def json_reader(file_info, options, progress_info, exit_event):
    
    # set the total size for progress report
    progress_info[1].value = os.path.getsize(file_info.path)
    decoder = json.JSONDecoder()
    
    with open(file_info.path, "r") as file_in:
        file_offset = 0
        offset      = 0
        foundStart  = False
        foundFirst  = False
        readMore    = False
        json_array  = False
        json_data   = ''
        while not exit_event.is_set():
            file_offset += offset
            progress_info[0].value = file_offset
            
            if foundStart is False or readMore:
                # Read the data in chunks, since the json module would just read the whole thing at once
                chunk = file_in.read(min(json_read_chunk_size, json_max_buffer_size - len(json_data)))
                if len(chunk) == 0:
                    break # end of file
                dataLength = len(json_data) + len(chunk) - offset
                if dataLength >= json_max_buffer_size:
                    raise Exception("Error: JSON max buffer size exceeded on file %s (from position %d). Use '--max-document-size' to extend your buffer." % (file_info.path, file_offset - len(json_data)))
                json_data = json_data[offset:] + chunk
                
                # reset offset
                offset = 0
            
            # read past leading whitespace
            offset = json.decoder.WHITESPACE.match(json_data, offset).end()
            if len(json_data) == 0:
                continue
            
            # check if we are in a newline-delimited or standard json array document
            if not foundStart:
                if len(json_data) < 2:
                    continue # go back and read more
                if json_data[offset] == "[": # read as a standard json array of dicts
                    foundStart = True
                    json_array = True
                    offset += 1
                elif json_data[offset] == "{": # read as a newline-terminates list of dicts
                    foundStart = True
                else:
                    raise RuntimeError("Error: JSON format not recognized - file does not begin with an object or array")
            
            # look for the end of the outer array or intermediate ',' character
            elif json_array:
                if json_data[offset] == "]":
                    json_data = json_data[offset + 1:]
                    break
                elif foundFirst:
                    if json_data[offset] == ",":
                        offset = json.decoder.WHITESPACE.match(json_data, offset + 1).end() # Read past the comma and any additional whitespace
                    else:
                        raise ValueError("Error: JSON format not recognized - expected ',' or ']' after object at byte %d in %s.%s file" % (offset + file_offset, file_info.db, file_info.table))
            
            # read a row
            try:
                row, offset = decoder.raw_decode(json_data, idx=offset)
                foundFirst = True
                yield row
            except (ValueError, IndexError):
                pass # did not find a complete JSON object in the buffer, read more
        
        # - try to read any remaining parts of the file
        
        file_offset += offset
        progress_info[0].value = file_offset
        chunk = file_in.read(min(json_read_chunk_size, json_max_buffer_size - len(json_data)))
        if len(json_data) + len(chunk) - offset >= json_max_buffer_size:
            raise Exception("Error: JSON max buffer size exceeded after data ended on file %s (from position %d). Use '--max-document-size' to extend your buffer." % (file_info.path, file_offset - len(json_data)))
        json_data = json_data[offset:] + chunk
        
        # - make sure only remaining data is whitespace
        
        while len(json_data) > 0:
            offset = json.decoder.WHITESPACE.match(json_data, 0).end()
            if offset != len(json_data):
                raise RuntimeError("Error: JSON format not recognized - extra characters found after end of data (position %d)" % (file_offset + offset))
            json_data = file_in.read(json_read_chunk_size)
    
    progress_info[0].value = progress_info[1].value

# Wrapper classes for the handling of unicode csv files
# Taken from https://docs.python.org/2/library/csv.html
class Utf8Recoder:
    def __init__(self, f):
        self.reader = codecs.getreader("utf-8")(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class Utf8CsvReader:
    def __init__(self, f, **kwargs):
        f = Utf8Recoder(f)
        self.reader = csv.reader(f, **kwargs)
        self.line_num = self.reader.line_num

    def next(self):
        row = self.reader.next()
        self.line_num = self.reader.line_num
        return [unicode(s, "utf-8") for s in row]

    def __iter__(self):
        return self

def open_csv_file(filename):
    if PY3:
        return open(filename, "r", encoding="utf-8", newline="")
    else:
        return open(filename, "r")

def csv_reader(task_queue, filename, db, table, options, progress_info, exit_event):
    object_buffers = []
    buffer_sizes = []

    # Count the lines so we can report progress
    # TODO: this requires us to make two passes on csv files
    line_count = 0
    with open_csv_file(file_info.path) as file_in:
        for i, l in enumerate(file_in):
            pass
        line_count = i + 1

    progress_info[1].value = line_count

    with open_csv_file(file_info.path) as file_in:
        if PY3:
            reader = csv.reader(file_in, delimiter=options.delimiter)
        else:
            reader = Utf8CsvReader(file_in, delimiter=options.delimiter)

        if not options.no_header:
            fields_in = next(reader)

        # Field names may override fields from the header
        if options.custom_header is not None:
            if not options.no_header and not options.quiet:
                print("Ignoring header row: %s" % str(fields_in))
            fields_in = options.custom_header
        elif options.no_header:
            raise RuntimeError("Error: No field name information available")

        for row in reader:
            file_line = reader.line_num
            progress_info[0].value = file_line
            if len(fields_in) != len(row):
                raise RuntimeError("Error: File '%s' line %d has an inconsistent number of columns" % (file_info.path, file_line))
            # We import all csv fields as strings (since we can't assume the type of the data)
            obj = dict(zip(fields_in, row))
            for key in list(obj.keys()): # Treat empty fields as no entry rather than empty string
                if len(obj[key]) == 0:
                    del obj[key]
            object_callback(obj, file_info.db, file_info.table, task_queue, object_buffers, buffer_sizes, options.fields, exit_event)

    if len(object_buffers) > 0:
        task_queue.put((file_info.db, file_info.table, object_buffers))

def table_worker(file_info, options, error_queue, warning_queue, progress_info, rows_written, exit_event):
    work_queue = Queue.Queue()
    try:
        # - ensure the db exists
        utils_common.retryQuery("ensure db: %s" % file_info.db, r.expr([file_info.db]).set_difference(r.db_list()).for_each(r.db_create(r.row)))
        
        # - ensure the table exists and is ready
        utils_common.retryQuery(
            "create table: %s.%s" % (file_info.db, file_info.table),
            r.expr([file_info.table]).set_difference(r.db(file_info.db).table_list()).for_each(r.db(file_info.db).table_create(r.row, **options.create_args))
        )
        utils_common.retryQuery("wait for %s.%s" % (file_info.db, file_info.table), r.db(file_info.db).table(file_info.table).wait(timeout=30))
        
        # - recreate secondary indexes - droping existing on the assumption they are wrong
        if options.sindexes:
            existing_indexes = utils_common.retryQuery("indexes from: %s.%s" % (file_info.db, file_info.table), r.db(file_info.db).table(file_info.table).index_list())
            try:
                created_indexes = []
                for index in file_info.indexes:
                    if index["index"] in existing_indexes: # drop existing versions
                        utils_common.retryQuery(
                            "drop index: %s.%s:%s" % (file_info.db, file_info.table, index["index"]),
                            r.db(file_info.db).table(file_info.table).index_drop(index["index"])
                        )
                    utils_common.retryQuery(
                        "create index: %s.%s:%s" % (file_info.db, file_info.table, index["index"]),
                        r.db(file_info.db).table(file_info.table).index_create(index["index"], index["function"])
                    )
                    created_indexes.append(index["index"])
                
                # wait for all of the created indexes to build
                utils_common.retryQuery(
                    "waiting for indexes on %s.%s" % (file_info.db, file_info.table),
                    r.db(file_info.db).table(file_info.table).index_wait(r.args(created_indexes))
                )
            except RuntimeError as e:
                ex_type, ex_class, tb = sys.exc_info()
                warning_queue.put((ex_type, ex_class, traceback.extract_tb(tb), file_info.path))
        
        # - start the writer thread
        writer_thread = threading.Thread(
            target=table_writer,
            name="%s.%s writer" % (file_info.db, file_info.table),
            kwargs={
                "file_info":         file_info,
                "options":           options,
                "task_queue":        work_queue,
                "error_queue":       error_queue,
                "rows_written":      rows_written
            }
        )
        writer_thread.start()
        
        # - setup the data source
        
        source = None
        if file_info.format == "json":
            source = json_reader(file_info, options, progress_info, exit_event)
        elif file_info.format == "csv":
            source = csv_reader(file_info, options, progress_info, exit_event)
        else:
            raise RuntimeError("Error: Unknown file format specified: %s" % file_info.format)
        
        # - read the source into the queue
        
        for row in source:
            work_queue.put(row)
        work_queue.put(StopIteration())
        
        # - wait for the worker
        writer_thread.join()
        
    
    # -- report relevent errors
    except InterruptedError:
        pass # Don't save interrupted errors, they are side-effects of signals
    except Exception as e:
        error_queue.put(Error(str(e), traceback.format_exc(), file_info.path))
    finally:
        work_queue.put(StopIteration())

__signalSeen = False
def abort_import(signum, frame, parent_pid, exit_event, task_queue, clients, interrupt_event):
    global __signalSeen
    # Only do the abort from the parent process
    if os.getpid() == parent_pid:
        if __signalSeen:
            # second time
            print("\nSecond terminate signal seen, aborting ungracefully")
            for worker in workers:
                try:
                    worker.terminate()
                except Exception as e:
                    print("Problem killing worker: %s" % str(e))
        else:
            print("\nTerminate signal seen, aborting")
            __signalSeen = True
            interrupt_event.set()
            exit_event.set()

def update_progress(progress_info, options):
    lowest_completion = 1.0
    for current, max_count in progress_info:
        curr_val = current.value
        max_val = max_count.value
        if curr_val < 0:
            lowest_completion = 0.0
        elif max_val <= 0:
            lowest_completion = 1.0
        else:
            lowest_completion = min(lowest_completion, float(curr_val) / max_val)

    if not options.quiet:
        utils_common.print_progress(lowest_completion, padding=2)

workers = []
def import_tables(options, files_info):
    global workers
    
    # Spaw a worker (reader + writer in threads in a external process) for each table, options.clients at a time
    error_queue = SimpleQueue()
    warning_queue = SimpleQueue()
    exit_event = multiprocessing.Event()
    interrupt_event = multiprocessing.Event()
    errors = []
    start_time = time.time()
    
    parent_pid = os.getpid()
    signal.signal(signal.SIGINT, lambda a, b: abort_import(a, b, parent_pid, exit_event, task_queue, client_procs, interrupt_event))
    
    try:
        progress_info = []
        rows_written = multiprocessing.Value(ctypes.c_longlong, 0)
        
        # - start the workers options.clients at a time
        filesLeft = len(files_info)
        for file_info in files_info:
            # add it to the queue
            progress = (
                multiprocessing.Value(ctypes.c_longlong, -1), # Current lines/bytes processed
                multiprocessing.Value(ctypes.c_longlong, 0)   # Total lines/bytes to process
            )
            progress_info.append(progress)
            
            worker = multiprocessing.Process(
                target=table_worker,
                name="worker %s.%s" % (file_info.db, file_info.table),
                args=(file_info, options, error_queue, warning_queue, progress, rows_written, exit_event)
            )
            worker.start()
            workers.append(worker)
            filesLeft -= 1
            
            # wait for there to be another opening
            while len(workers) == min(options.clients, filesLeft):
                time.sleep(0.1)
                for worker in workers[:]:
                    if not worker.is_alive():
                        workers.remove(worker)
                
                # monitor the error queue while we are waiting
                while not error_queue.empty():
                    exit_event.set()
                    errors.append(error_queue.get())
                
                # update the progress bar
                update_progress(progress_info, options)
        
        # - wait for the last of the workers to complete
        while workers:
            time.sleep(0.1)
            for worker in workers[:]:
                if not worker.is_alive():
                    workers.remove(worker)
            
            # monitor the error queue while we are waiting
            while not error_queue.empty():
                exit_event.set()
                errors.append(error_queue.get())
            
            # update the progress bar
            update_progress(progress_info, options)

        # If we were successful, make sure 100% progress is reported
        if len(errors) == 0 and not interrupt_event.is_set() and not options.quiet:
            utils_common.print_progress(1.0, padding=2)
        
        plural = lambda num, text: "%d %s%s" % (num, text, "" if num == 1 else "s")
        if not options.quiet:
            # Continue past the progress output line
            print("\n  %s imported to %s in %.2f secs" % (plural(rows_written.value, "row"), plural(len(files_info), "table"), time.time() - start_time))
    finally:
        signal.signal(signal.SIGINT, signal.SIG_DFL)

    if interrupt_event.is_set():
        raise RuntimeError("Interrupted")

    if len(errors) != 0:
        for error in errors:
            print("%s" % error.message, file=sys.stderr)
            if options.debug and error.traceback:
                print("  Traceback:\n%s" % error.traceback, file=sys.stderr)
            if len(error.file) == 4:
                print("  In file: %s" % error.file, file=sys.stderr)
        raise RuntimeError("Errors occurred during import")

    if not warning_queue.empty():
        while not warning_queue.empty():
            warning = warning_queue.get()
            print("%s" % warning[1], file=sys.stderr)
            if options.debug:
                print("%s traceback: %s" % (warning[0].__name__, warning[2]), file=sys.stderr)
            if len(warning) == 4:
                print("In file: %s" % warning[3], file=sys.stderr)
        raise RuntimeError("Warnings occurred during import")

def import_directory(options):
    # Make sure this isn't a pre-`reql_admin` cluster - which could result in data loss
    # if the user has a database named 'rethinkdb'
    utils_common.check_minimum_version("1.6")
    
    # Scan for all files, make sure no duplicated tables with different formats
    dbs = False
    files_info = {} # (db, table) => {file:, format:, db:, table:, info:}
    files_ignored = []
    for root, dirs, files in os.walk(options.directory):
        if not dbs:
            files_ignored.extend([os.path.join(root, f) for f in files])
            # The first iteration through should be the top-level directory, which contains the db folders
            dbs = True
            
            # don't recurse into folders not matching our filter
            db_filter = set([db_table[0] for db_table in options.db_tables or []])
            if db_filter:
                for dirName in dirs[:]: # iterate on a copy
                    if dirName not in db_filter:
                        dirs.remove(dirName)
        else:
            if dirs:
                files_ignored.extend([os.path.join(root, d) for d in dirs])
                del dirs[:]
            
            db = os.path.basename(root)
            for filename in files:
                table, ext = os.path.splitext(filename)
                table = os.path.basename(table)
                
                if ext not in [".json", ".csv", ".info"]:
                    files_ignored.append(os.path.join(root, filename))
                elif ext == ".info":
                    pass # Info files are included based on the data files
                elif not os.path.exists(os.path.join(root, table + ".info")):
                    files_ignored.append(os.path.join(root, filename))
                else:
                    # ensure we don't have a duplicate
                    if (db, table) in files_info:
                        raise RuntimeError("Error: Duplicate db.table found in directory tree: %s.%s" % (db, table))
                    
                    # apply db/table filters
                    if options.db_tables:
                        for filter_db, filter_table in options.db_tables:
                            if db == filter_db and filter_table in (None, table):
                                break # either all tables in this db, or specific pair
                        else:
                            files_ignored.append(os.path.join(root, filename))
                            continue # not a chosen db/table
                    
                    # collect the info
                    primary_key = None
                    indexes = []
                    try:
                        with open(os.path.join(root, table + ".info"), "r") as info_file:
                            metadata = json.load(info_file)
                            if "primary_key" in metadata:
                                primary_key = metadata["primary_key"]
                            if "indexes" in metadata:
                                indexes = metadata["indexes"]
                    except OSError:
                        files_ignored.append(os.path.join(root, f))
                    
                    files_info[(db, table)] = SourceFile(path=os.path.join(root, filename), db=db, table=table, format=ext.lstrip("."), primary_key=primary_key, indexes=indexes)
    
    # create missing dbs
    needed_dbs = set([x[0] for x in files_info])
    if "rethinkdb" in needed_dbs:
        raise RuntimeError("Error: Cannot import tables into the system database: 'rethinkdb'")
    utils_common.retryQuery("ensure dbs: %s" % ", ".join(needed_dbs), r.expr(needed_dbs).set_difference(r.db_list()).for_each(r.db_create(r.row)))
    
    # check for existing tables, or if --force is enabled ones with mis-matched primary keys
    existing_tables = dict([
        ((x["db"], x["name"]), x["primary_key"]) for x in
        utils_common.retryQuery("list tables", r.db("rethinkdb").table("table_config").pluck(["db", "name", "primary_key"]))
    ])
    already_exist = []
    for db, table, primary_key in ((x.db, x.table, x.primary_key) for x in files_info.values()):
        if (db, table) in existing_tables:
            if not options.force:
                already_exist.append("%s.%s" % (db, table))

            elif primary_key != existing_tables[(db, table)]:
                raise RuntimeError("Error: Table '%s.%s' already exists with a different primary key: %s (expected: %s)" % (db, table, existing_tables[(db, table)], primary_key))
    
    if len(already_exist) == 1:
        raise RuntimeError("Error: Table '%s' already exists, run with --force to import into the existing table" % already_exist[0])
    elif len(already_exist) > 1:
        already_exist.sort()
        raise RuntimeError("Error: The following tables already exist, run with --force to import into the existing tables:\n  %s" % "\n  ".join(already_exist))

    # Warn the user about the files that were ignored
    if len(files_ignored) > 0:
        print("Unexpected files found in the specified directory.  Importing a directory expects", file=sys.stderr)
        print(" a directory from `rethinkdb export`.  If you want to import individual tables", file=sys.stderr)
        print(" import them as single files.  The following files were ignored:", file=sys.stderr)
        for f in files_ignored:
            print("%s" % str(f), file=sys.stderr)
    
    # start the imports
    
    import_tables(options, files_info.values())

def import_file(options):
    db, table = options.import_table
    if db == "rethinkdb":
        raise RuntimeError("Error: Cannot import a table into the system database: 'rethinkdb'")
    
    # Make sure this isn't a pre-`reql_admin` cluster - which could result in data loss
    # if the user has a database named 'rethinkdb'
    utils_common.check_minimum_version("1.6")
    
    # Ensure that the database and table exist with the right primary key
    utils_common.retryQuery("create db %s" % db, r.expr([db]).set_difference(r.db_list()).for_each(r.db_create(r.row)))
    tableInfo = None
    try:
        tableInfo = utils_common.retryQuery('table info: %s.%s' % (db, table), r.db(db).table(table).info())
    except r.ReqlOpFailedError:
        pass # table does not exist
    if tableInfo:
        if not force:
            raise RuntimeError("Error: Table `%s.%s` already exists, run with --force if you want to import into the existing table" % (db, table))
        if "primary_key" in create_args:
            if create_args["primary_key"] != tableInfo["primary_key"]:
                raise RuntimeError("Error: Table already exists with a different primary key")
    else:
        if "primary_key" not in create_args and not quiet:
            print("no primary key specified, using default primary key when creating table")
        utils_common.retryQuery("create table: %s.%s" % (db, table), r.db(db).table_create(table, **create_args))
        tableInfo = utils_common.retryQuery("table info: %s.%s" % (db, table), r.db(db).table(table).info())
    
    # Make this up so we can use the same interface as with an import directory
    file_info = SourceFile(
        path=options.file,
        db=db,
        table=table,
        format=options.format,
        primary_key=tableInfo["primary_key"],
        indexes=[]
    )
    
    import_tables(options, [file_info])

def main(argv=None, prog=None):
    if argv is None:
        argv = sys.argv[1:]
    try:
        options = parse_options(argv, prog=prog)
    except RuntimeError as ex:
        print("Usage:\n%s" % usage, file=sys.stderr)
        print(ex, file=sys.stderr)
        return 1

    try:
        start_time = time.time()
        if options.directory:
            import_directory(options)
        elif options.file:
            import_file(options)
        else:
            raise RuntimeError("Error: Neither --directory or --file specified")
    except RuntimeError as ex:
        print(ex, file=sys.stderr)
        if str(ex) == "Warnings occurred during import":
            return 2
        return 1
    if not options.quiet:
        print("  Done (%d seconds)" % (time.time() - start_time))
    return 0

if __name__ == "__main__":
    sys.exit(main())

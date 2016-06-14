#!/usr/bin/env python

'''`rethinkdb import` loads data into a RethinkDB cluster'''

from __future__ import print_function

import codecs, collections, csv, ctypes, datetime, json, mmap, multiprocessing
import optparse, os, re, signal, sys, threading, time, traceback

try:
    xrange
except NameError:
    xrange = range
try:
    from multiprocessing import SimpleQueue
except ImportError:
    from multiprocessing.queues import SimpleQueue
try:
    import Queue
except ImportError:
    import queue as Queue

# Used because of API differences in the csv module, taken from
# http://python3porting.com/problems.html
PY3 = sys.version > "3"

#json parameters
json_read_chunk_size = 32 * 1024
json_max_buffer_size = 128 * 1024 * 1024
max_nesting_depth = 100
default_batch_size = 200

from . import utils_common
r = utils_common.r

Error = collections.namedtuple("Error", ["message", "traceback", "file"])

class NeedMoreData(Exception):
    pass

class SourceFile(object):
    format           = None
    
    db               = None
    table            = None
    
    batch_size       = None
    primary_key      = None
    indexes          = None
    buffer_size      = None

    _source_file     = None # open filehandle for the source, must have buffering disabled
    
    _lock            = None # lock necessary for working with any of the following
    _source_complete = None # c_bool, True when file has been completely read into the buffer
    _buffer_complete = None # c_bool, True when buffer has no more complete rows
    _buffer          = None # read/write mmap
    _buffer_pos      = None # c_ulonglong, current read position
    _buffer_end      = None # c_ulonglong, last byte of data
    _source_read     = None # c_ulonglong, bytes read from source to buffer
    _read_count      = None # c_ulonglong, number of times source has been read
    
    start_time       = None
    end_time         = None
    
    _bytes_size      = None
    _bytes_processed = None # -1 until started
    
    _total_rows      = None # -1 until known
    _rows_read       = None
    _rows_written    = None
    
    def __init__(self, source, db, table, batch_size=None, primary_key=None, indexes=None, buffer_size=None):
        assert self.format is not None, 'Class %s must have a format' % self.__class__.__name
        
        # source
        sourceLength = 0
        if hasattr(source, 'read'):
            self._source_file = source
        else:
            try:
                sourceLength = os.path.getsize(source)
                if sourceLength == 0:
                    raise ValueError('Source is zero-length: %s' % source)
                self._source_file = open(source, mode='r', buffering=0)
            except IOError as e:
                raise ValueError('Unable to open source file "%s": %s' % (str(source), str(e)))
        
        # other input data
        self.db             = db
        self.table          = table
        
        # defaulted input
        if batch_size:
            try:
                self.batch_size = int(batch_size)
                assert self.batch_size > 0
            except Exception:
                raise ValueError('Wrong format for batch_size, must be positive int: %s' % batch_size)
        else:
            self.batch_size = default_batch_size
        self.primary_key    = str(primary_key) if primary_key else 'id'
        self.indexes        = indexes or []
        if buffer_size:
            try:
                self.buffer_size = int(buffer_size)
            except Exception:
                raise ValueError('Wrong format for buffer_size: %s' % buffer_size)
        else:
            self.buffer_size = json_max_buffer_size
        
        # internal values
        self._lock            = multiprocessing.RLock()
        self._source_complete = multiprocessing.Value(ctypes.c_bool, False)
        self._buffer_complete = multiprocessing.Value(ctypes.c_bool, True)
        self._buffer          = mmap.mmap(-1, length=self.buffer_size, access=mmap.ACCESS_WRITE)
        self._buffer_pos      = multiprocessing.Value(ctypes.c_ulonglong, 0)
        self._buffer_end      = multiprocessing.Value(ctypes.c_ulonglong, 0)
        self._source_read     = multiprocessing.Value(ctypes.c_ulonglong, 0)
        self._read_count      = multiprocessing.Value(ctypes.c_ulonglong, 0)
        
        # reporting information
        self._bytes_size      = multiprocessing.Value(ctypes.c_longlong, sourceLength)
        self._bytes_processed = multiprocessing.Value(ctypes.c_longlong, -1)
        
        self._total_rows      = multiprocessing.Value(ctypes.c_longlong, -1)
        self._rows_read       = multiprocessing.Value(ctypes.c_longlong, 0)
        self._rows_written    = multiprocessing.Value(ctypes.c_longlong, 0)
        
        with self._lock: # needless, but the right habit
            self.setup_file()
    
    def fill_buffer(self):
        '''Fill the buffer (mmap) from the source'''
        if self._buffer_complete.value:
            with self._lock:
                if self._source_complete.value:
                    raise StopIteration()
                
                self._read_count.value += 1 # allow subclasses to keep cache correctly
                
                # copy the data left in the buffer to the front part
                if self._buffer_pos.value < self._buffer_end.value:
                    self._buffer.move(0, self._buffer_pos.value, self._buffer_end.value - self._buffer_pos.value)
                self._buffer_end.value = self._buffer_end.value - self._buffer_pos.value
                self._buffer_pos.value = 0
                
                # set the write-head to this new end
                self._buffer.seek(self._buffer_end.value)
                
                # write the next chunk into the buffer
                chunkSize = self.buffer_size - self._buffer_end.value
                self._buffer.write(self._source_file.read(chunkSize))
                readBytes = self._buffer.tell() - self._buffer_end.value
                self._source_read.value += readBytes
                self._buffer_end.value = self._buffer.tell()
                
                # reset buffer markers
                self._buffer_complete.value = False
                
                # mark ourselves complete if we have reached EOF
                if readBytes == 0:
                    self._source_complete.value = True
                    raise StopIteration()
            
    def setup_file(self):
        '''Do any setup on the file that needs to be done'''
        with self._lock:
            self.fill_buffer()
    
    def teardown_file(self):
        '''Do any final actions on the file that might need to be done'''
    
    def setup_table(self, options):
        '''Ensure that the db, table, and indexes exist and are correct'''
        
        # - ensure the db exists
        utils_common.retryQuery("ensure db: %s" % self.db, r.expr([self.db]).set_difference(r.db_list()).for_each(r.db_create(r.row)))
        
        # - ensure the table exists and is ready
        utils_common.retryQuery(
            "create table: %s.%s" % (self.db, self.table),
            r.expr([self.table]).set_difference(r.db(self.db).table_list()).for_each(r.db(self.db).table_create(r.row, **options.create_args))
        )
        utils_common.retryQuery("wait for %s.%s" % (self.db, self.table), r.db(self.db).table(self.table).wait(timeout=30))
        
        # - ensure that the primary key on the table is correct
        primaryKey = utils_common.retryQuery(
            "primary key %s.%s" % (self.db, self.table),
            r.db(self.db).table(self.table).info()["primary_key"],
        )
        if primaryKey != self.primary_key:
            raise RuntimeError("Error: table %s.%s primary key was `%s` rather than the expected: %s" % (self.db, table.table, primaryKey, self.primary_key))
        
        # - recreate secondary indexes - droping existing on the assumption they are wrong
        if options.sindexes:
            existing_indexes = utils_common.retryQuery("indexes from: %s.%s" % (self.db, self.table), r.db(self.db).table(self.table).index_list())
            try:
                created_indexes = []
                for index in self.indexes:
                    if index["index"] in existing_indexes: # drop existing versions
                        utils_common.retryQuery(
                            "drop index: %s.%s:%s" % (self.db, self.table, index["index"]),
                            r.db(self.db).table(self.table).index_drop(index["index"])
                        )
                    utils_common.retryQuery(
                        "create index: %s.%s:%s" % (self.db, self.table, index["index"]),
                        r.db(self.db).table(self.table).index_create(index["index"], index["function"])
                    )
                    created_indexes.append(index["index"])
                
                # wait for all of the created indexes to build
                utils_common.retryQuery(
                    "waiting for indexes on %s.%s" % (self.db, self.table),
                    r.db(self.db).table(self.table).index_wait(r.args(created_indexes))
                )
            except RuntimeError as e:
                ex_type, ex_class, tb = sys.exc_info()
                warning_queue.put((ex_type, ex_class, traceback.extract_tb(tb), self._source_file.name))
    
    def batches(self, batch_size=None):
        '''Generator of batches of lines from the source. Handles necessary locking'''
        if not batch_size:
            batch_size = self.batch_size
        
        while True:
            if self._buffer_complete.value and self._source_complete.value:
                if batch:
                    yield batch
                    batch = []
                raise StopIteration() # so we don't have to grab the lock
            
            batch = []
            error = None
            with self._lock:
                self.fill_buffer() # raises StopIteration when needed
                
                for _ in xrange(self.batch_size):
                    try:
                        row = self.get_line()
                        # ToDo: validate the line
                        batch.append(row)
                    except NeedMoreData:
                        self._buffer_complete.value = True
                        break
                self._rows_read.value += len(batch)
            
            # without the lock
            if batch:
                self.bytes_processed = self._source_read.value - (self._buffer_end.value - self._buffer_pos.value)
                yield batch
                batch = []
    
    def get_line(self):
        '''Returns a single line from the file, assumes we have the _lock'''
        raise NotImplementedError('This needs to be implemented on the %s subclass' % self.format)
    
    # - bytes
    @property
    def bytes_size(self):
        return self._bytes_size.value
    @bytes_size.setter
    def bytes_size(self, value):
        self._bytes_size.value = value
    
    @property
    def bytes_processed(self):
        return self._bytes_processed.value
    @bytes_processed.setter
    def bytes_processed(self, value):
        self._bytes_processed.value = value
    
    # - rows
    @property
    def total_rows(self):
        return self._total_rows.value
    @total_rows.setter
    def total_rows(self, value):
        self._total_rows.value = value
    
    @property
    def rows_read(self):
        return self._rows_read.value
    @rows_read.setter
    def rows_read(self, value):
        self._rows_read.value = value
    
    @property
    def rows_written(self):
        return self._rows_written.value
    def add_rows_written(self, increment): # we have multiple writers to coordinate
        with self._rows_written.get_lock():
            self._rows_written.value += increment
    
    # - percent done
    @property
    def percentDone(self):
        '''return a float between 0 and 1 for a reasonable guess of percentage complete'''
        # assume that reading takes 50% of the time and writing the other 50%
        completed = 0.0 # of 2.0
        
        # - add read percentage
        if self._bytes_size.value <= 0 or self._bytes_size.value <= self._bytes_processed.value:
            completed += 1.0
        elif self.__bytes_read.value < 0 and self.__total_rows.value >= 0:
            # done by rows read
            if self._rows_read > 0:
                completed += float(self._rows_read) / float(self._total_rows.value)
        else:
            # done by bytes read
            if self._bytes_processed.value > 0:
                completed += float(self._bytes_processed.value) / float(self._bytes_size.value)
        read = completed
        
        # - add written percentage
        if self._rows_read.value or self._rows_written.value:
            totalRows = float(self._total_rows.value)
            if totalRows == 0:
                completed += 1.0
            elif totalRows < 0:
                # a guesstimate
                perRowSize = float(self._bytes_processed.value) / float(self._rows_read.value)
                totalRows = float(self._rows_read.value) + (float(self._bytes_size.value - self._bytes_processed.value) / perRowSize)
                completed += float(self._rows_written.value) / totalRows
            else:
                # accurate count
                completed += float(self._rows_written.value) / totalRows
        
        # - return the value
        return completed * 0.5

class JsonSourceFile(SourceFile):
    format      = 'json'
    
    decoder     = json.JSONDecoder()
    json_array  = False
    found_first = None # c_bool
    
    buffer_str  = None # local string to make json happy
    buffer_read = None # the _read_count corresponding to our buffer_str
    
    def __init__(self, *args, **kwargs):
        super(JsonSourceFile, self).__init__(*args, **kwargs)
        self.found_first = multiprocessing.Value(ctypes.c_bool, False)
    
    def setup_file(self):
        '''Advance to the first row'''
        with self._lock:
            # move though any leading whitespace
            while self._source_complete.value is False:
                self.fill_buffer()
                self._buffer_pos.value = json.decoder.WHITESPACE.match(self._buffer, 0).end()
                if self._buffer_pos.value != self._buffer_end.value:
                    break
            
            # check the first character
            if self._buffer_pos.value == self._buffer_end.value:
                raise ValueError("Error: JSON file was empty of content")
            elif self._buffer[0] == "[":
                self.json_array = True
                self._buffer_pos.value = 1
            elif self._buffer[0] != "{":
                raise ValueError("Error: JSON format not recognized - file does not begin with an object or array")
    
    def get_line(self):
        '''Get a line from the buffer. This should only be called from batches()'''
        
        try:
            waste = 'before %d ' % self._buffer_pos.value
            # read over any whitespace
            self._buffer_pos.value = json.decoder.WHITESPACE.match(self._buffer, self._buffer_pos.value).end()
            waste += '%d ' % self._buffer_pos.value
            if self._buffer_pos.value >= self._buffer_end.value:
                raise NeedMoreData()
            
            # read over a comma if we are not the first item in an array
            waste += '<%s> ' % self._buffer[self._buffer_pos.value]
            if self.json_array and self.found_first.value and self._buffer_end.value > self._buffer_pos.value and self._buffer[self._buffer_pos.value] == ",":
                self._buffer_pos.value += 1
                waste += 'moved'
            if self._buffer_pos.value >= self._buffer_end.value:
                raise NeedMoreData()
            
            # read over any whitespace
            self._buffer_pos.value = json.decoder.WHITESPACE.match(self._buffer, self._buffer_pos.value).end()
            if self._buffer_pos.value >= self._buffer_end.value:
                raise NeedMoreData()
            
            # copy the buffer into a string so we can use it - raw_decode enforces string-only sources
            if self.buffer_str is None or self.buffer_read != self._read_count.value:
                self.buffer_str = self._buffer[0:self._buffer_end.value]
                self.buffer_read = self._read_count.value
            
            # parse and return an object
            try:
                row, self._buffer_pos.value = self.decoder.raw_decode(self.buffer_str, idx=self._buffer_pos.value)
                self.found_first.value = True
                return row
            except (ValueError, IndexError) as e:
                raise NeedMoreData()
        
        except NeedMoreData:
            self.buffer_str = None
            raise

def table_worker(source, options, name, error_queue, warning_queue, exit_event):
    signal.signal(signal.SIGINT, signal.SIG_IGN) # workers should ignore these
    conflict_action = "replace" if options.force else "error"
    
    db    = source.db
    table = source.table
    tbl   = r.db(db).table(table)
    
    for batch in source.batches():
        if exit_event.is_set():
            return
        try:
            # validate batch contents
            assert isinstance(batch, list), 'batch was not a list: %s' % batch
            assert len(batch) > 0, 'empty batch'
            assert len(batch) <= options.batch_size, 'batch was too big: %d (%d)' % (len(batch), options.batch_size)
            for row in batch:
                assert isinstance(row, dict), 'row was not dict: %s' % row
            
            # write the batch to the database
            try:
                res = utils_common.retryQuery(
                    "write batch to %s.%s" % (db, table),
                    tbl.insert(r.expr(batch, nesting_depth=max_nesting_depth), durability=options.durability, conflict=conflict_action)
                )
                
                if res["errors"] > 0:
                    raise RuntimeError("Error when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                modified = res["inserted"] + res["replaced"] + res["unchanged"]
                if modified != len(batch):
                    raise RuntimeError("The inserted/replaced/unchanged number did not match when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                
                source.add_rows_written(modified)
                
            except r.ReqlError:
                # the error might have been caused by a comm or temporary error causing a partial batch write
                
                for row in batch:
                    if not source.primary_key in row:
                        raise RuntimeError("Connection error while importing.  Current row does not have the specified primary key (%s), so cannot guarantee absence of duplicates" % source.primary_key)
                    res = None
                    if conflict_action == "replace":
                        res = utils_common.retryQuery(
                            "write row to %s.%s" % (db, table),
                            tbl.insert(r.expr(row, nesting_depth=max_nesting_depth), durability=options.durability, conflict=conflict_action)
                        )
                    else:
                        existingRow = utils_common.retryQuery(
                            "read row from %s.%s" % (db, table),
                            tbl.get(row[source.primary_key])
                        )
                        if not existingRow:
                            res = utils_common.retryQuery(
                                "write row to %s.%s" % (db, table),
                                tbl.insert(r.expr(row, nesting_depth=max_nesting_depth), durability=options.durability, conflict=conflict_action)
                            )
                        elif existingRow != row:
                            raise RuntimeError("Duplicate primary key `%s`:\n%s\n%s" % (source.primary_key, str(row), str(existingRow)))
                    
                    if res["errors"] > 0:
                        raise RuntimeError("Error when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                    if res["inserted"] + res["replaced"] + res["unchanged"] != 1:
                        raise RuntimeError("The inserted/replaced/unchanged number was not 1 when inserting on '%s.%s': %s" % (db, table, res))
                    source.add_rows_written(1)
        except Exception as e:
            error_queue.put(Error(str(e), traceback.format_exc(), "%s.%s" % (db , table)))
            exit_event.set()

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
    
    parser.add_option("--clients",           dest="clients",    metavar="CLIENTS",    default=8,      help="client connections to use (default: 8)", type="pos_int")
    parser.add_option("--hard-durability",   dest="durability", action="store_const", default="soft", help="use hard durability writes (slower, uses less memory)", const="hard")
    parser.add_option("--force",             dest="force",      action="store_true",  default=False,  help="import even if a table already exists, overwriting duplicate primary keys")
    
    parser.add_option("--writers-per-table", dest="writers",    metavar="WRITERS",    default=multiprocessing.cpu_count(), help=optparse.SUPPRESS_HELP, type="pos_int")
    parser.add_option("--batch-size",        dest="batch_size", metavar="BATCH",      default=default_batch_size,          help=optparse.SUPPRESS_HELP, type="pos_int")
    
    # Replication settings
    replicationOptionsGroup = optparse.OptionGroup(parser, "Replication Options")
    replicationOptionsGroup.add_option("--shards",   dest="create_args", metavar="SHARDS",   help="shards to setup on created tables (default: 1)",   type="pos_int", action="add_key")
    replicationOptionsGroup.add_option("--replicas", dest="create_args", metavar="REPLICAS", help="replicas to setup on created tables (default: 1)", type="pos_int", action="add_key")
    parser.add_option_group(replicationOptionsGroup)

    # Directory import options
    dirImportGroup = optparse.OptionGroup(parser, "Directory Import Options")
    dirImportGroup.add_option("-d", "--directory",      dest="directory", metavar="DIRECTORY",   default=None, help="directory to import data from")
    dirImportGroup.add_option("-i", "--import",         dest="db_tables", metavar="DB|DB.TABLE", default=[],   help="restore only the given database or table (may be specified multiple times)", action="append", type="db_table")
    dirImportGroup.add_option("--no-secondary-indexes", dest="sindexes",  action="store_false",  default=None, help="do not create secondary indexes")
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
        
        if options.sindexes is None:
            options.sindexes = True
        
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
            options.format = os.path.splitext(options.file)[1].lstrip('.')
        
        # import_table
        if options.import_table:
            res = utils_common._tableNameRegex.match(options.import_table)
            if res and res.group("table"):
                options.import_table = utils_common.DbTable(res.group("db"), res.group("table"))
            else:
                parser.error("Invalid --table option: %s" % options.import_table)
        
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
            if not options.import_table:
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
def table_writer(tables, options, work_queue, error_queue, warning_queue, exit_event):
    signal.signal(signal.SIGINT, signal.SIG_IGN) # workers should ignore these
    db = table = batch = None
    
    try:
        conflict_action = "replace" if options.force else "error"
        while not exit_event.is_set():
            # get a batch
            db, table, batch = work_queue.get()
            
            # shut down when appropriate
            if isinstance(batch, StopIteration):
                return
            
            # find the table we are working on
            table_info = tables[(db, table)]
            tbl = r.db(db).table(table)
            
            # write the batch to the database
            try:
                res = utils_common.retryQuery(
                    "write batch to %s.%s" % (db, table),
                    tbl.insert(r.expr(batch, nesting_depth=max_nesting_depth), durability=options.durability, conflict=conflict_action)
                )
                
                if res["errors"] > 0:
                    raise RuntimeError("Error when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                modified = res["inserted"] + res["replaced"] + res["unchanged"]
                if modified != len(batch):
                    raise RuntimeError("The inserted/replaced/unchanged number did not match when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                
                table_info.add_rows_written(modified)
                
            except r.ReqlError:
                # the error might have been caused by a comm or temporary error causing a partial batch write
                
                for row in batch:
                    if not table_info.primary_key in row:
                        raise RuntimeError("Connection error while importing.  Current row does not have the specified primary key (%s), so cannot guarantee absence of duplicates" % table_info.primary_key)
                    res = None
                    if conflict_action == "replace":
                        res = utils_common.retryQuery(
                            "write row to %s.%s" % (db, table),
                            tbl.insert(r.expr(row, nesting_depth=max_nesting_depth), durability=durability, conflict=conflict_action)
                        )
                    else:
                        existingRow = utils_common.retryQuery(
                            "read row from %s.%s" % (db, table),
                            tbl.get(row[table_info.primary_key])
                        )
                        if not existingRow:
                            res = utils_common.retryQuery(
                                "write row to %s.%s" % (db, table),
                                tbl.insert(r.expr(row, nesting_depth=max_nesting_depth), durability=durability, conflict=conflict_action)
                            )
                        elif existingRow != row:
                            raise RuntimeError("Duplicate primary key `%s`:\n%s\n%s" % (table_info.primary_key, str(row), str(existingRow)))
                    
                    if res["errors"] > 0:
                        raise RuntimeError("Error when importing into table '%s.%s': %s" % (db, table, res["first_error"]))
                    if res["inserted"] + res["replaced"] + res["unchanged"] != 1:
                        raise RuntimeError("The inserted/replaced/unchanged number was not 1 when inserting on '%s.%s': %s" % (db, table, res))
                    table_info.add_rows_written(1)
            
    except Exception as e:
        error_queue.put(Error(str(e), traceback.format_exc(), "%s.%s" % (db , table)))
        exit_event.set()

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
    reader = None
    
    def __init__(self, iterable, **kwargs):
        self.reader = csv.reader(Utf8Recoder(iterable), **kwargs)
    
    @property
    def line_num(self):
        return self.reader.line_num
    
    def next(self):
        return [unicode(s, "utf-8") for s in self.reader.next()]
    
    def __iter__(self):
        return self

def csv_reader(table, options, exit_event):
    with open(filename, "r", encoding="utf-8", newline="") if PY3 else open(table.path, "r") as file_in:
        # - Count the lines so we can report progress # TODO: work around the double pass
        for i, _ in enumerate(file_in):
            pass
        table.total_rows = i + 1
        
        # rewind the file
        file_in.seek(0)
        
        if PY3:
            reader = csv.reader(file_in, delimiter=options.delimiter)
        else:
            reader = Utf8CsvReader(file_in, delimiter=options.delimiter)
        
        # - Get the header information for column names
        if not options.no_header:
            fields_in = next(reader)

        # Field names may override fields from the header
        if options.custom_header is not None:
            if not options.no_header and not options.quiet:
                print("Ignoring header row: %s" % str(fields_in))
            fields_in = options.custom_header
        elif options.no_header:
            raise RuntimeError("Error: No field name information available")
        
        # - Read in the data
        
        batch = []
        for row in reader:
            if exit_event.is_set():
                break
            
            # update progress
            file_line = reader.line_num
            table.rows_read = file_line
            
            if len(fields_in) != len(row):
                raise RuntimeError("Error: File '%s' line %d has an inconsistent number of columns" % (table.path, file_line))
            
            # We import all csv fields as strings (since we can't assume the type of the data)
            row = dict(zip(fields_in, row))
            
            # Treat empty fields as no entry rather than empty string
            for key in list(row.keys()):
                if len(row[key]) == 0:
                    del row[key]
                elif options.fields and key not in options.fields:
                    del row[key]
            
            batch.append(row)
            if len(batch) >= options.batch_size:
                table.rows_read += len(batch)
                yield batch
                batch = []
        
        if batch:
            table.rows_read += len(batch)
            yield batch

def abort_import(pools, exit_event, interrupt_event):
    if interrupt_event.is_set():
        # second time
        print("\nSecond terminate signal seen, aborting ungracefully")
        for pool in pools:
            for worker in pool:
                worker.terminate()
                worker.join(.1)
    else:
        print("\nTerminate signal seen, aborting")
        interrupt_event.set()
        exit_event.set()

def update_progress(tables, options, done_event, exit_event, sleep=0.2):
    signal.signal(signal.SIGINT, signal.SIG_IGN) # workers should not get these
    if options.quiet:
        raise Exception('update_progress called when --quiet was set')
    
    # give weights to each of the tables based on file size
    totalSize = sum([x.bytes_size for x in tables])
    for table in tables:
        table.weight = float(table.bytes_size) / totalSize
    
    lastComplete = None
    startTime    = time.time()
    readWrites   = collections.deque(maxlen=5) # (time, read, write)
    readWrites.append((startTime, 0, 0))
    readRate     = None
    writeRate    = None
    while True:
        try:
            if done_event.is_set() or exit_event.is_set():
                break
            complete = read = write = 0
            currentTime = time.time()
            for table in tables:
                complete += table.percentDone * table.weight
                if options.debug:
                    read     += table.rows_read
                    write    += table.rows_written
            readWrites.append((currentTime, read, write))
            if complete != lastComplete:
                timeDelta = readWrites[-1][0] - readWrites[0][0]
                if options.debug and len(readWrites) > 1:
                    readRate  = max((readWrites[-1][1] - readWrites[0][1]) / timeDelta, 0)
                    writeRate = max((readWrites[-1][2] - readWrites[0][2]) / timeDelta, 0)
                utils_common.print_progress(complete, indent=2, read=readRate, write=writeRate)
                lastComplete = complete
            time.sleep(sleep)
        except KeyboardInterrupt: break
        except Exception as e:
            if options.debug:
                print(e)
                traceback.print_exc()

def import_tables(options, files_info):
    start_time = time.time()
    
    tables = dict(((x.db, x.table), x) for x in files_info) # (db, table) => table
    
    work_queue      = SimpleQueue()
    error_queue     = multiprocessing.Queue()
    warning_queue   = SimpleQueue()
    done_event      = multiprocessing.Event()
    exit_event      = multiprocessing.Event()
    interrupt_event = multiprocessing.Event()
    
    pools = []
    progressBar = None
    progressBarSleep = 0.2
    
    # setup KeyboardInterupt handler
    signal.signal(signal.SIGINT, lambda a, b: abort_import(pools, exit_event, interrupt_event))
    
    try:
        # - start the progress bar
        if not options.quiet:
            progressBar = multiprocessing.Process(
                target=update_progress,
                name="progress bar",
                args=(files_info, options, done_event, exit_event, progressBarSleep)
            )
            progressBar.start()
            pools.append([progressBar])
        
        # - setup all the tables
        for source in files_info:
            source.setup_table(options)
        
        # - start the workers
        workers = []
        pools.append(workers)
        for source in files_info:
            for i in range(16):
                name = "table writer %s.%s %d" % (source.db, source.table, i + 1)
                worker = multiprocessing.Process(
                    target=table_worker, name=name,
                    kwargs={
                        "source":source, "options":options, 'name':name,
                        "error_queue":error_queue, "warning_queue":warning_queue,
                        "exit_event":exit_event
                    }
                )
                workers.append(worker)
                worker.start()
        
        # - wait for all the workers to complete
        while workers:
            for worker in workers[:]:
                worker.join(.1)
                if not worker.is_alive():
                    workers.remove(worker)
        
        # - stop the progress bar
        if progressBar:
            done_event.set()
            progressBar.join(progressBarSleep * 2)
            if not interrupt_event.is_set():
                utils_common.print_progress(1, indent=2)
            if progressBar.is_alive():
                progressBar.terminate()
        
        # - drain the error_queue
        errors = []
        while True:
            try:
                errors.append(error_queue.get(True, timeout=.0001))
            except Queue.Empty:
                break
        
        # - if successful, make sure 100% progress is reported
        if len(errors) == 0 and not interrupt_event.is_set() and not options.quiet:
            utils_common.print_progress(1.0, indent=2)
        
        plural = lambda num, text: "%d %s%s" % (num, text, "" if num == 1 else "s")
        if not options.quiet:
            # Continue past the progress output line
            print("\n  %s imported to %s in %.2f secs" % (plural(sum(x.rows_written for x in files_info), "row"), plural(len(files_info), "table"), time.time() - start_time))
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
                path = os.path.join(root, filename)
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
                    
                    sourceType = JsonSourceFile # Todo: the logic for this
                    
                    files_info[(db, table)] = sourceType(
                        source=path,
                        db=db, table=table,
                        batch_size=options.batch_size,
                        primary_key=primary_key,
                        indexes=indexes
                    )
    
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
        if not options.force:
            raise RuntimeError("Error: Table `%s.%s` already exists, run with --force if you want to import into the existing table" % (db, table))
        if "primary_key" in options.create_args:
            if options.create_args["primary_key"] != tableInfo["primary_key"]:
                raise RuntimeError("Error: Table already exists with a different primary key")
    else:
        if "primary_key" not in options.create_args and not options.quiet:
            print("no primary key specified, using default primary key when creating table")
        utils_common.retryQuery("create table: %s.%s" % (db, table), r.db(db).table_create(table, **options.create_args))
        tableInfo = utils_common.retryQuery("table info: %s.%s" % (db, table), r.db(db).table(table).info())
    
    # Make this up so we can use the same interface as with an import directory
    table = SourceFile(
        path=options.file,
        db=db,
        table=table,
        format=options.format,
        primary_key=tableInfo["primary_key"],
    )
    
    import_tables(options, [table])

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

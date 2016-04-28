#!/usr/bin/env python

'''`rethinkdb restore` loads data into a RethinkDB cluster from an archive'''

from __future__ import print_function

import copy, datetime, os, shutil, sys, tarfile, tempfile, time
from . import utils_common, net

usage = "rethinkdb restore FILE [-c HOST:PORT] [--tls-cert FILENAME] [-p] [--password-file FILENAME] [--clients NUM] [--shards NUM_SHARDS] [--replicas NUM_REPLICAS] [--force] [-i (DB | DB.TABLE)]..."
help_epilog = '''
FILE:
    the archive file to restore data from;
    if FILE is -, use standard input (note that
    intermediate files will still be written to
    the --temp-dir directory)

EXAMPLES:

rethinkdb restore rdb_dump.tar.gz -c mnemosyne:39500
  Import data into a cluster running on host 'mnemosyne' with a client port at 39500 using
  the named archive file.

rethinkdb restore rdb_dump.tar.gz -i test
  Import data into a local cluster from only the 'test' database in the named archive file.

rethinkdb restore rdb_dump.tar.gz -i test.subscribers -c hades -p
  Import data into a cluster running on host 'hades' which requires a password from only
  a specific table from the named archive file.

rethinkdb restore rdb_dump.tar.gz --clients 4 --force
  Import data to a local cluster from the named archive file using only 4 client connections
  and overwriting any existing rows with the same primary key.
'''

def parse_options(argv):
    parser = utils_common.CommonOptionsParser(usage=usage, epilog=help_epilog)
    
    parser.add_option("-i", "--import",         dest="tables",          metavar="DB|DB.TABLE", default=[],    help="limit restore to the given database or table (may be specified multiple times)", action="append", type="db_table")

    parser.add_option("--shards",               dest="shards",          metavar="SHARDS",      default=0,     help="the number of shards to set on tables created", type="int")
    parser.add_option("--replicas",             dest="replicas",        metavar="REPLICAS",    default=0,     help="the number of replicas to set on tables created", type="int")
    
    parser.add_option("--temp-dir",             dest="temp_dir",        metavar="DIR",         default=None,  help="directory to use for intermediary results")
    parser.add_option("--clients",              dest="clients",         metavar="CLIENTS",     default=8,     help="client connections to use (default: 8)", type="int")
    parser.add_option("--hard-durability",      dest="hard",            action="store_true",   default=False, help="use hard durability writes (slower, but less memory)")
    parser.add_option("--force",                dest="force",           action="store_true",   default=False, help="import data even if a table already exists")
    parser.add_option("--no-secondary-indexes", dest="create_sindexes", action="store_false",  default=True,  help="do not create secondary indexes for the restored tables")
    
    options, args = parser.parse_args(argv)

    # -- Check validity of arguments
    
    # - archive
    if len(args) == 0:
        parser.error("Archive to import not specified. Provide an archive file created by rethinkdb-dump.")
    elif len(args) != 1:
        parser.error("Only one positional argument supported")
    options.in_file = args[0]
    if options.in_file == '-':
        options.in_file = sys.stdin
    else:
        if not os.path.isfile(options.in_file):
            parser.error("Archive file does not exist: %s" % options.in_file)
        options.in_file = os.path.realpath(options.in_file)
    
    # - temp_dir

    if options.temp_dir:
        if not os.path.isdir(options.temp_dir):
            parser.error("Temporary directory doesn't exist or is not a directory: %s" % options.temp_dir)
        if not os.access(res["temp_dir"], os.W_OK):
            parser.error("Temporary directory inaccessible: %s" % options.temp_dir)

    return options

def do_unzip(temp_dir, options):
    if not options.quiet:
        print("Unzipping archive file...")
    start_time = time.time()
    original_dir = os.getcwd()
    sub_path = None

    tables_to_export = set(options.tables)
    members = []

    def parse_path(member):
        path = os.path.normpath(member.name)
        if member.isdir():
            return ('', '', '')
        if not member.isfile():
            raise RuntimeError("Error: Archive file contains an unexpected entry type")
        if not os.path.realpath(os.path.abspath(path)).startswith(os.getcwd()):
            raise RuntimeError("Error: Archive file contains unexpected absolute or relative path")
        path, table_file = os.path.split(path)
        path, db = os.path.split(path)
        path, base = os.path.split(path)
        return (base, db, os.path.splitext(table_file)[0])

    # If the in_file is a fileobj (e.g. stdin), stream it to a seekable file
    # first so that the code below can seek in it.
    tar_temp_file_path = None
    if options.in_file is sys.stdin:
        fileobj = options.in_file
        fd, tar_temp_file_path = tempfile.mkstemp(suffix=".tar.gz", dir=options.temp_dir)
        # Constant memory streaming, buf == "" on EOF
        with os.fdopen(fd, "w", 65536) as f:
            buf = None
            while buf != "":
                buf = fileobj.read(65536)
                f.write(buf)
        name = tar_temp_file_path
    else:
        name = options.in_file

    try:
        with tarfile.open(name, "r:*") as f:
            os.chdir(temp_dir)
            try:
                for member in f:
                    base, db, table = parse_path(member)
                    if len(base) > 0:
                        if sub_path is None:
                            sub_path = base
                        elif sub_path != base:
                            raise RuntimeError("Error: Archive file has an unexpected directory structure (%s vs %s)" % (sub_path, base))

                        if len(tables_to_export) == 0 or \
                           (db, table) in tables_to_export or \
                           (db, None) in tables_to_export:
                            members.append(member)

                if sub_path is None:
                    raise RuntimeError("Error: Archive file had no files")

                f.extractall(members=members)

            finally:
                os.chdir(original_dir)
    finally:
        if tar_temp_file_path is not None:
            os.remove(tar_temp_file_path)

    if not options.quiet:
        print("  Done (%d seconds)" % (time.time() - start_time))
    return os.path.join(temp_dir, sub_path)

def do_import(temp_dir, options):
    from . import _import
    
    options = copy.copy(options)
    options.directory = temp_dir
    
    if not options.quiet:
        print("Importing from directory...")
    
    # ToDo: make the following work
    
    for db, table in options["tables"]:
        if table is None:
            import_args.extend(["--import", db])
        else:
            import_args.extend(["--import", "%s.%s" % (db, table)])
    
    try:
        _import.import_directory(options)
    except RuntimeError as ex:
        if str(ex) == "Warnings occurred during import":
            raise RuntimeError("Warning: import did not create some secondary indexes.")
        else:
            raise RuntimeError("Error: import failed")
    # 'Done' message will be printed by the import script

def run_rethinkdb_import(options):
    # Create a temporary directory to store the extracted data
    temp_dir = tempfile.mkdtemp(dir=options["temp_dir"])
    res = -1

    try:
        sub_dir = do_unzip(temp_dir, options)
        do_import(sub_dir, options)
    except KeyboardInterrupt:
        time.sleep(0.2)
        raise RuntimeError("Interrupted")
    finally:
        shutil.rmtree(temp_dir)

def main(argv=None):
    if argv is None:
        argv = sys.argv[1:]
    try:
        options = parse_options(argv)
    except RuntimeError as ex:
        print("Usage: %s" % usage, file=sys.stderr)
        print(ex, file=sys.stderr)
        return 1

    try:
        start_time = time.time()
        run_rethinkdb_import(options)
    except RuntimeError as ex:
        print(ex, file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    exit(main())

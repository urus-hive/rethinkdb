#!/usr/bin/env python

'''`rethinkdb dump` creates an archive of data from a RethinkDB cluster'''

from __future__ import print_function

import datetime, os, shutil, sys, tarfile, tempfile, time

from . import utils_common, net
r = utils_common.r

usage = "rethinkdb dump [-c HOST:PORT] [-p] [--password-file FILENAME] [--tls-cert FILENAME] [-f FILE] [--clients NUM] [-e (DB | DB.TABLE)]..."
help_epilog = '''
EXAMPLES:
rethinkdb dump -c mnemosyne:39500
  Archive all data from a cluster running on host 'mnemosyne' with a client port at 39500.

rethinkdb dump -e test -f rdb_dump.tar.gz
  Archive only the 'test' database from a local cluster into a named file.

rethinkdb dump -c hades -e test.subscribers -p
  Archive a specific table from a cluster running on host 'hades' which requires a password.
'''

def parse_options(argv):
    parser = utils_common.CommonOptionsParser(usage=usage, epilog=help_epilog)
    
    parser.add_option("-f", "--file",     dest="out_file", metavar="FILE",            default=None,  help='file to write archive to (defaults to rethinkdb_dump_DATE_TIME.tar.gz);\nif FILE is -, use standard output (note that intermediate files will still be written to the --temp-dir directory)')
    parser.add_option("-e", "--export",   dest="tables",   metavar="(DB | DB.TABLE)", default=[],    help='limit dump to the given database or table (may be specified multiple times)', action="append")

    parser.add_option("--temp-dir",       dest="temp_dir", metavar="directory",       default=None,  help='the directory to use for intermediary results')
    parser.add_option("--overwrite-file", dest="overwrite_file",                      default=False, help="overwrite -f/--file if it exists", action="store_true")
    parser.add_option("--clients",        dest="clients",  metavar="NUM",             default=3,     help='number of tables to export simultaneously (default: 3)', type="int")
    
    options, args = parser.parse_args(argv)
    # Check validity of arguments
    if len(args) != 0:
        raise RuntimeError("Error: No positional arguments supported. Unrecognized option(s): %s" % args)

    # Verify valid output file
    if sys.platform.startswith('win32') or sys.platform.startswith('cygwin'):
        options["temp_filename"] = "rethinkdb_dump_%s" % datetime.datetime.today().strftime("%Y-%m-%dT%H-%M-%S")
    else:
        options["temp_filename"] = "rethinkdb_dump_%s" % datetime.datetime.today().strftime("%Y-%m-%dT%H:%M:%S")

    if options.out_file == "-":
        options.out_file = sys.stdout
        options.quiet = True 
    elif options.out_file is None:
        options.out_file = os.realpath(res["temp_filename"] + ".tar.gz")
    else:
        options.out_file = os.path.realpath(options.out_file)
    if os.path.exists(options.out_file) and not options.overwrite_file:
        parser.error("Output file already exists: %s" % options.out_file)
    if os.path.exists(options.out_file) and not os.path.isfile(options.out_file):
        parser.error("There is a non-file at the -f/--file location: %s" % options.out_file)

    # Verify valid client count
    if options.clients < 1:
        raise RuntimeError("Error: invalid number of clients (%d), must be greater than zero" % options.clients)

    # Make sure the temporary directory exists and is accessible
    if options.temp_dir is not None:
        if not os.path.isdir(options.temp_dir):
            raise RuntimeError("Error: Temporary directory doesn't exist or is not a directory: %s" % options.temp_dir)
        if not os.access(options.temp_dir, os.W_OK):
            raise RuntimeError("Error: Temporary directory inaccessible: %s" % options.temp_dir)
    
    return options

def do_export(temp_dir, options):
    from . import _export
    
    if not options["quiet"]:
        print("Exporting to directory...")
    export_args = [
        "--connect", "%s:%s" % (options["host"], options["port"]),
        "--directory", os.path.join(temp_dir, options["temp_filename"]),
        "--clients", str(options["clients"]),
        "--tls-cert", options["tls_cert"]
    ]
    if options["password"]:
        export_args.append("--password")
    if options["password-file"]:
        export_args.extend(["--password-file", options["password-file"]])
    for table in options["tables"]:
        export_args.extend(["--export", table])
    if options["debug"]:
        export_args.extend(["--debug"])
    if options["quiet"]:
        export_args.extend(["--quiet"])
    
    if _export.main(export_args) != 0:
        raise RuntimeError("Error: export failed")
    # 'Done' message will be printed by the export script (unless options["quiet"])

def do_zip(temp_dir, options):
    if not options["quiet"]:
        print("Zipping export directory...")
    start_time = time.time()

    # Below,` tarfile.open()` forces us to set either `name` or `fileobj`,
    # depending on whether the output is a real file or an open file object.
    try:
        archive = None
        if hasattr(options["out_file"], 'read'):
            archive = tarfile.open(fileobj=options["out_file"], mode="w:gz")
        else:
            archive = tarfile.open(name=options["out_file"], mode="w:gz")
        
        data_dir = os.path.join(temp_dir, options["temp_filename"])
        for curr, subdirs, files in os.walk(data_dir):
            for data_file in files:
                fullPath = os.path.join(data_dir, curr, data_file)
                archivePath = os.path.relpath(fullPath, temp_dir)
                archive.add(fullPath, arcname=archivePath)
                os.unlink(fullPath)
    finally:
        archive.close()

    if not options["quiet"]:
        print("  Done (%d seconds)" % (time.time() - start_time))

def run_rethinkdb_export(options):
    # Create a temporary directory to store the intermediary results
    temp_dir = tempfile.mkdtemp(dir=options.temp_dir)

    if not options["quiet"]:
        # Print a warning about the capabilities of dump, so no one is confused (hopefully)
        print("NOTE: 'rethinkdb-dump' saves data and secondary indexes, but does *not* save")
        print(" cluster metadata.  You will need to recreate your cluster setup yourself after")
        print(" you run 'rethinkdb-restore'.")

    try:
        do_export(temp_dir, options)
        do_zip(temp_dir, options)
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
        run_rethinkdb_export(options)
    except RuntimeError as ex:
        print(ex, file=sys.stderr)
        return 1
    return 0

if __name__ == "__main__":
    exit(main())

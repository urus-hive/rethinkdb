#!/usr/bin/env python

'''Dispatcher for interactive functions such as repl and backup'''

import os, sys
from . import utils_common

def startInterpreter(argv):
    import code, readline, optparse
    
    banner = '-- The RethinkDB driver has been imported as r --'
    
    # -- get host/port setup
    
    # - parse command line
    parser = optparse.OptionParser(prog=args[0])
    parser.add_option("-c", "--connect",  dest="host", metavar="HOST:PORT", default="localhost:28015", help="host and client port of a rethinkdb server")
    
    parser.add_option("-p", "--password", dest="password", default=None, action="store_true", help="interactively prompt for a password  to connect")
    parser.add_option("--password-file",  dest="password", metavar="FILENAME", default=None, help="read password required to connect from file")
    parser.add_option("--tls-cert",       dest="tls_cert", metavar="TLS_CERT", default=None, help="certificate file to use for TLS encryption")

    options, args = parser.parse_args(argv)
    
    # -- start interpreter
    code.interact(banner=banner, local={'r':utils_common.r, 'rethinkdb':utils_common.r})
    
if __name__ == '__main__':
    if __package__ is None:
        __package__ = 'rethinkdb'
    
    # -- figure out which mode we are in
    modes = ['dump', 'export', 'import', 'index_rebuild', 'repl', 'restore']
    
    if len(sys.argv) < 2 or sys.argv[1] not in modes:
        sys.exit('ERROR: Must be called with one of the following verbs: %s' % ', '.join(modes))
    
    verb = sys.argv[1]
    prog = 'python -m rethinkdb'
    if sys.version_info < (2, 7):
        prog += '.__main__'
    argv = [prog + ' ' + verb] + sys.argv[2:] # a bit of a hack, but it works well where we need it
    
    if verb == 'dump':
        from . import _dump
        exit(_dump.main(argv))
    elif verb == 'export':
        from . import _export
        exit(_export.main(argv))
    elif verb == 'import':
        from . import _import
        exit(_import.main(argv))
    elif verb == 'index_rebuild':
        from . import _index_rebuild
        exit(_index_rebuild.main(argv))
    elif verb == 'repl':
        startInterpreter(argv)
    elif verb == 'restore':
        from . import _restore
        exit(_restore.main(argv))

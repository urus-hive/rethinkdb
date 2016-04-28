'''Common tools for interactive utilities such as repl or backup'''

from __future__ import print_function

import collections, copy, getpass, inspect, optparse, os, re, socket, string, sys

from . import net
r = net.Connection._r

_connection_info = None # set by CommonOptionsParser

# This file contains common functions used by the import/export/dump/restore scripts


def parse_db_table(item):
    res = __tableNameRegex.match(item)
    if not res:
        raise RuntimeError("Error: Invalid 'db' or 'db.table' name: %s" % item)
    return (res.group('db'), res.group('table'))

def parse_db_table_options(db_table_options):
    res = []
    for item in db_table_options:
        res.append(parse_db_table(item))
    return res

# This function is used to wrap rethinkdb calls to recover from connection errors
# The first argument to the function is an output parameter indicating if progress
# has been made since the last call.  This is passed as an array so it works as an
# output parameter. The first item in the array is compared each attempt to check
# if progress has been made.
# Using this wrapper, the given function will be called until 5 connection errors
# occur in a row with no progress being made.  Care should be taken that the given
# function will terminate as long as the progress parameter is changed.
def rdb_call_wrapper(context, fn, *args, **kwargs):
    i = 0
    max_attempts = 5
    progress = [None]
    while True:
        last_progress = copy.deepcopy(progress[0])
        try:
            conn = getConnection()
            return fn(progress, conn, *args, **kwargs)
        except socket.error as ex:
            i = i + 1 if progress[0] == last_progress else 0
            if i == max_attempts:
                raise RuntimeError("Connection error during '%s': %s" % (context, ex.message))
        except (r.ReqlError, r.ReqlDriverError) as ex:
            raise RuntimeError("ReQL error during '%s': %s" % (context, ex.message))

def print_progress(ratio):
    total_width = 40
    done_width = int(ratio * total_width)
    undone_width = total_width - done_width
    print("\r[%s%s] %3d%%" % ("=" * done_width, " " * undone_width, int(100 * ratio)), end=' ')
    sys.stdout.flush()

def check_minimum_version(progress, conn, minimum_version):
    stringify_version = lambda v: '.'.join(map(str, v))
    parsed_version = None
    try:
        version = r.db('rethinkdb').table('server_status')[0]['process']['version'].run(conn)
        matches = re.match(r'rethinkdb (\d+)\.(\d+)\.(\d+).*', version)
        if matches == None:
            raise RuntimeError("invalid version string format")
        parsed_version = tuple(int(num) for num in matches.groups())
        if parsed_version < minimum_version:
            raise RuntimeError("incompatible version")
    except (RuntimeError, TypeError, r.ReqlRuntimeError):
        if parsed_version is None:
            message = "Error: Incompatible server version found, expected >= %s" % \
                stringify_version(minimum_version)
        else:
            message = "Error: Incompatible server version found (%s), expected >= %s" % \
                (stringify_version(parsed_version), stringify_version(minimum_version))
        raise RuntimeError(message)

__connectRegex = re.compile(r'^\s*(?P<hostname>[\w\.-]+)(:(?P<port>\d+))?\s*$')
__tableNameRegex = re.compile(r'^(?P<db>\w+)(\.(?P<table>\w+))?$')
DbTable = collections.namedtuple('DbTable', ['db', 'table'])
class CommonOptionsParser(optparse.OptionParser, object):
    
    def format_epilog(self, formatter):
        return self.epilog or ''
    
    def __init__(self, *args, **kwargs):
        
        # -- Option Checkers and Callbacks
        
        def checkTlsOption(option, opt, value):
            value = str(value)
            if os.path.isfile(value):
                return {"ca_certs": os.path.realpath(value)}
            else:
                raise optparse.OptionValueError("option %s value is not a file: %r" % (opt, value))
        
        def checkDbTableOption(option, opt, value):
            res = __tableNameRegex.match(item)
            if not res:
                raise RuntimeError("Error: Invalid 'db' or 'db.table' name: %s" % item)
            return DbTable(res.group('db'), res.group('table'))
        
        def combinedConnectAction(option, opt_str, value, parser):
            res = __connectRegex.match(value)
            if not res:
                raise optparse.OptionValueError("Invalid 'host:port' format: %s" % value)
            if res.group('hostname'):
                parser.values.hostname = res.group('hostname')
            if res.group('port'):
                parser.values.driver_port = int(res.group('port'))
        
        def passwordOption(option, opt_str, value, parser):
            parser.values.password = getpass.getpass("Password for `admin`: ")
        
        def passwordFileOption(option, opt_str, value, parser):
            if not os.path.isfile(value):
                raise optparse.OptionValueError("option %s value is not a file: %r" % (opt_str, value))
            try:
                with open(value, 'r') as passwordFile:
                    parser.values.password = passwordFile.read().rstrip('\n')
            except IOError:
                raise optparse.OptionValueError('Error: bad value for --password-file: %s' % value)
        
        # -- setup custom Options object
        
        class CommonOptionChecker(optparse.Option):
            TYPES = optparse.Option.TYPES + ("tls_cert", "db_table")
            TYPE_CHECKER = copy.copy(optparse.Option.TYPE_CHECKER)
            TYPE_CHECKER["tls_cert"] = checkTlsOption
            TYPE_CHECKER["db_table"] = checkDbTableOption
        kwargs['option_class'] = CommonOptionChecker
        
        # - default description to the module's __doc__
        if not 'description' in kwargs:
            # get calling module
            caller = inspect.getmodule(inspect.stack()[1][0])
            if caller.__doc__:
                kwargs['description'] = caller.__doc__
        
        super(CommonOptionsParser, self).__init__(*args, **kwargs)
        
        # -- add common options
        
        self.add_option("-q", "--quiet", dest="quiet", default=False, action="store_true", help='suppress non-error messages')
        self.add_option("--debug", dest="debug", default=False, action="store_true", help=optparse.SUPPRESS_HELP)
        
        connectionGroup = optparse.OptionGroup(self, 'Connection options')
        connectionGroup.add_option("-c", "--connect",  dest="driver_port", metavar="HOST:PORT", help="host and client port of a rethinkdb node to connect (default: localhost:%d)" % net.DEFAULT_PORT, action="callback", callback=combinedConnectAction, type='string')
        connectionGroup.add_option("--driver-port",    dest="driver_port", metavar="PORT",      help="driver port of a rethinkdb server", type="int", default=os.environ.get('RETHINKDB_DRIVER_PORT', net.DEFAULT_PORT))
        connectionGroup.add_option("--host-name",      dest="hostname",    metavar="HOST",      help="host and driver port of a rethinkdb server", default=os.environ.get('RETHINKDB_HOSTNAME', 'localhost'))
        connectionGroup.add_option("-u", "--user",     dest="user",        metavar="USERNAME",  help="user name to connect as", default=os.environ.get('RETHINKDB_USER', 'admin'))
        connectionGroup.add_option("-p", "--password", dest="password",                         help='interactively prompt for a password for the connection', action="callback", callback=passwordOption)
        connectionGroup.add_option("--password-file",  dest="password",    metavar="PSWD_FILE", help='read the connection password from a file', action="callback", callback=passwordFileOption, type='string')
        connectionGroup.add_option("--tls-cert",       dest="ssl",         metavar="TLS_CERT",  help="certificate file to use for TLS encryption.", type="tls_cert")
        self.add_option_group(connectionGroup)
    
    def parse_args(self, *args, **kwargs):
        global _connection_info
        
        if 'RETHINKDB_DRIVER_PORT' in os.environ:
            try:
                value = int(os.environ['RETHINKDB_DRIVER_PORT'])
                assert value > 0
            except (ValueError, AssertionError):
                self.error('ENV variable RETHINKDB_DRIVER_PORT is not a useable integer: %s' % os.environ['RETHINKDB_DRIVER_PORT'])
        
        options, args = super(CommonOptionsParser, self).parse_args(*args, **kwargs)
        _connection_info = {
            'host':     options.hostname,
            'port':     options.driver_port,
            'user':     options.user,
            'password': options.password,
            'ssl':      options.ssl
        }
        return options, args

__conn = None
def getConnection():
    global __conn
    assert _connection_info is not None, 'If you are using this non-interactively you need to set _connection_info yourself'
    
    # check if shard connection is still good
    if __conn:
        try:
            r.expr(0).run(__conn)
        except r.ReqlError:
            __conn = None
    
    if not __conn:
        __conn = r.connect(**_connection_info)
        
    return __conn

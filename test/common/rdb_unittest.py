#!/usr/bin/env python
# Copyright 2015-2016 RethinkDB, all rights reserved.

import inspect, itertools, os, pprint, random, shutil, sys, time, unittest, warnings

try:
    long
except NameError:
    long = int
try:
    unicode
except NameError:
    unicode = str

import driver, utils

def main():
    runner = unittest.TextTestRunner(verbosity=2)
    unittest.main(argv=[sys.argv[0]], testRunner=runner)

class BadTableException(AssertionError):
    pass

class BadDataException(AssertionError):
    pass

class TableManager():
    '''Manages a single table to allow for clean re-use between tests'''
    
    # - settings
    
    tableName     = None
    dbName        = None
    
    primaryKey    = None
    shards        = 1
    replicas      = 1
    durability    = 'hard'
    writeAcks     = 'majority'
    
    minRecords    = None # minimum number of records to fill with
    minFillSecs   = None # minumim seconds to fill
    
    # - running variables
    
    records       = None  # if set, minRecords and minFillSecs are ignored 
    rangeStart    = None  # first key in the range of data
    rangeEnd      = None  # last key in the range of data
    
    # - internal cache values
    
    _table        = None
    _saved_config = None
    
    __initalized  = False
    
    # --
    
    def __init__(self, tableName, dbName, records=None, minRecords=None, minFillSecs=None, primaryKey=None, durability=None, writeAcks=None, shards=None, replicas=None):
        
        # -- initial values
        
        # - table
        assert tableName is not None, 'tableName value required (got None)'
        self.tableName = str(tableName)
        
        # - dbName
        self.dbName = str(dbName)
        
        # - records/minRecords/minFillSecs
        
        if records is not None:
            assert minRecords is None and minFillSecs is None, "records can't be used with either minRecords or minFillSecs"
            try:
                self.records = int(records)
            except Exception:
                raise ValueError('Bad records value: %r' % records)
        else:
            if minRecords is not None:
                try:
                    self.minRecords = int(minRecords)
                    assert self.minRecords > 0
                except Exception:
                    raise ValueError('Bad minRecords value: %r' % minRecords)
            if minFillSecs is not None:
                try:
                    self.minFillSecs = float(minFillSecs)
                    assert self.minFillSecs > 0
                except Exception:
                    raise ValueError('Bad minFillSecs value: %r' % minFillSecs)
        
        # - primaryKey
        self.primaryKey = primaryKey
        
        # - durability
        if durability is not None:
            self.durability = str(durability)
        
        # - writeAcks
        if writeAcks is not None:
            self.writeAcks = str(writeAcks)
        
        # - shards
        if shards is not None:
            try:
                self.shards = int(shards)
                assert self.shards > 0
            except Exception:
                raise ValueError('Bad shards value: %r' % shards)
        
        # - replicas
        if replicas is not None:
            try:
                self.replicas = int(replicas)
                assert self.replicas > 0
            except Exception:
                raise ValueError('Bad replicas value: %r' % replicas)
    
    def defaultSettings(self, conn, dbName, records, minRecords, minFillSecs, primaryKey, durability, writeAcks, shards, replicas):
        '''Set all of the non-set vaules to defaults, plus __conn'''
        
        # - conn
        assert conn is not None, 'conn value required (got None)'
        if hasattr(conn, 'reconnect'):
            self.__conn = conn
        elif hasattr(conn, '__call__'):
            self.__conn = conn
        else:
            raise ValueError('Bad conn value: %r' % conn)
        
        # -- defaults
        
        if not self.dbName:
            self.dbName = dbName
        if not self.records:
            self.records = records or 0
        if not self.minRecords:
            self.minRecords = minRecords
        if not self.minFillSecs:
            self.minFillSecs = minFillSecs
        if not self.primaryKey:
            self.primaryKey = primaryKey or 'id'
        if not self.durability:
            self.durability = durability or 'hard'
        if not self.writeAcks:
            self.writeAcks = writeAcks or 'majority'
        if not self.shards:
            self.shards = shards or 1
        if not self.replicas:
            self.replicas = replicas or 1
        
        # -- sanity checks
        
        assert isinstance(self.dbName, (str, unicode))
        assert isinstance(self.tableName, (str, unicode))
        assert isinstance(self.primaryKey, (str, unicode))
        assert self.durability in ('hard', 'soft')
        assert self.writeAcks in ('single', 'majority')
        
        assert isinstance(self.records, (int, long, None.__class__))
        if self.records is not None:
            self.records >= 0
        assert isinstance(self.minRecords, (int, long, None.__class__))
        if self.minRecords is not None:
            self.minRecords > 0
        assert isinstance(self.minFillSecs, (int, long, float, None.__class__))
        if self.minFillSecs is not None:
            self.minFillSecs > 0
        assert isinstance(self.shards, (int, long))
        assert self.shards >= 1
        assert isinstance(self.replicas, (int, long))
        assert self.replicas >= 1
    
    @property
    def conn(self):
        if not self.__conn:
            raise Exception('conn is not defined. This must be setup using setup() before this is ready to use')
        elif hasattr(self.__conn, '__call__'):
            return self.__conn()
        else:
            return self.__conn
    
    @property
    def table(self):
        if self._table:
            return self._table
        assert all([self.dbName, self.tableName])
        self._table = self.conn._r.db(self.dbName).table(self.tableName)
        return self._table
    
    def check(self, repair=False):
        self._checkTable(repair=repair)
        try:
            self._checkData(repair=repair)
        except BadDataException as e:
            if not repair:
                raise e
            else:
                # something went wrong repairing the data, nuke and pave
                self._checkTable(repair='force')
                self.__class__.__initalized = False
                self._checkData(repair=repair)
        self.table.wait().run(self.conn)
    
    def _checkTable(self, repair=False):
        '''Ensures that the table is in place with the correct settings'''
        
        r = self.conn._r
        
        if not repair:
            # -- check-only
            
            # - db
            res = r.db_list().run(self.conn)
            if self.dbName not in res:
                raise BadTableException('Missing db: %s' % self.dbName)
            
            # - table existance
            if not self.tableName in r.db(self.dbName).table_list().run(self.conn):
                raise BadTableException('Missing table: %s' % self.tableName)
            
            tableInfo = self.table.config().run(self.conn)
            pprint.pprint(tableInfo)
            # - primary key
            if tableInfo['primary_key'] != self.primaryKey:
                raise BadTableException('Expected primary key: %s but got: %s' % (self.primaryKey, tableInfo['primary_key']))
            
            # - secondary indexes
            if tableInfo['indexes']:
                raise BadTableException('Unexpected secondary indexes: %s' % tableInfo['indexes'])
            
            # - durability/writeAcks
            if tableInfo['durability'] != self.durability:
                raise BadTableException('Expected durability: %s got: %s' % (self.durability, tableInfo['durability']))
            if tableInfo['write_acks'] != self.writeAcks:
                raise BadTableException('Expected write_acks: %s got: %s' % (self.writeAcks, tableInfo['write_acks']))
            
            # - sharding/replication
            if len(tableInfo['shards']) != self.shards:
                raise BadTableException('Expected shards: %d got: %d' % (len(self.shards, tableInfo['shards'])))
            for shard in tableInfo['shards']:
                if len(shard['replicas']) != self.replicas:
                    raise BadTableException('Expected all shards to have %s replicas, at least one has %d:\n%s' % (self.replicas, len(shard['replicas']), pprint.pformat(tableInfo)))
            
            # - status
            statusInfo = self.table.status().run(self.conn)
            if not statusInfo['status']['all_replicas_ready']:
                raise BadTableException('Table did not show all_replicas_ready:\n%s' % pprint.pformat(statusInfo))
            
        else:
            # -- repair
        
            # - db
            r.expr([self.dbName]).set_difference(r.db_list()).for_each(r.db_create(r.row)).run(self.conn)
            
            # - forceRedo - delete table
            if repair == 'force':
                r.expr([self.tableName]).set_difference(r.db(self.dbName).table_list()).for_each(r.db(self.dbName).table_delete(r.row)).run(self.conn)
            
            # - table/primary key
            primaryKeys = r.db('rethinkdb').table('table_config').filter({'db':self.dbName, 'name':self.tableName}).pluck('primary_key')['primary_key'].coerce_to('array').run(self.conn)
                
            if primaryKeys != [self.primaryKey]:
                # bad primary key, drop the table if it exists and create it with the proper key
                if len(primaryKeys) == 1:
                    r.db(self.dbName).table_drop(self.tableName).run(self.conn)
                elif len(primaryKeys) > 1:
                    raise BadTableException('Somehow there was were multiple tables named %s.%s' %  (self.dbName, self.tableName))
                r.db(self.dbName).table_create(self.tableName, primary_key=self.primaryKey).run(self.conn)
                self.table.wait().run(self.conn)
            
            # - remove secondary indexes - todo: make this actually be able to reset indexes
            self.table.index_status().pluck('index')['index'].for_each(self.table.index_drop(r.row)).run(self.conn)
            
            # - durability/writeAcks
            configInfo = self.table.config().run(self.conn)
            if configInfo['durability'] != self.durability or configInfo['write_acks'] != self.writeAcks:
                res = self.table.config().update({'durability':self.durability, 'write_acks':self.writeAcks}).run(self.conn)
                if res['errors'] != 0:
                    raise BadTableException('Failed updating table metadata: %s' % str(res))
            
            # - sharding/replication
            shardInfo   = configInfo['shards']
            serverNames = list(r.db('rethinkdb').table('server_status')['name'].run(self.conn))
            if len(shardInfo) != self.shards or not all([len(x['replicas']) == self.replicas for x in shardInfo]):
                if len(serverNames) < self.shards * self.replicas:
                    raise BadTableException('Cluster does not have enough servers to only put one shard on each: %d vs %d * %d' % (len(serverNames), self.shards, self.replicas))
                
                replicas  = iter(serverNames[self.shards:])
                shardPlan = []
                for primaryName in serverNames[:self.shards]:
                    chosenReplicas = [replicas.next().name for _ in range(0, self.replicas - 1)]
                    shardPlan.append({'primary_replica':primaryName, 'replicas':[primaryName] + chosenReplicas})
                
                res = r.db(self.dbName).table(self.tableName).config().update({'shards':shardPlan}).run(self.conn)
                if res['errors'] != 0:
                    raise BadTableException('Failed updating shards: %s' % str(res))
        
        # -- wait for table to be ready
        
        self.table.wait().run(self.conn)
    
    def _fillInitialData(self, minRecords=None, minFillSecs=None):
        '''Fill the table with the initial set of data, and record the number of records. Records look like: { self.primaryKey: integer }'''
        print('Filling %s.%s' % (self.dbName, self.tableName))
        # - short circut for empty tables
        if not any([self.records, self.minRecords, self.minFillSecs]):
            self.table.delete().run(self.conn)
            self.records    = 0
            self.rangeStart = None
            self.rangeEnd   = None
            return
        
        r = self.conn._r
        
        # - ensure the table is empty
        assert self.table.count().run(self.conn) == 0, 'The table %s.%s was not empty' % (self.dbName, self.tableName)
        
        # - default input
        if minRecords is None:
            minRecords = self.minRecords
        if minFillSecs is None:
            minFillSecs = self.minFillSecs
        
        # - fill table
        rangeStart = 1
        records = 0
        rangeEnd = None
        fillMinTime = time.time() + self.minFillSecs if self.minFillSecs else None
        
        while all([minRecords is None or rangeStart < minRecords, minFillSecs is None or time.time() < fillMinTime, self.records is None or records < self.records]):
            # keep filling until we have satisfied all requirements
            
            stepSize = min(100, minRecords or 100) if self.records is None else min(100, self.records - records or 1)
            # ToDo: optimize stepSize on the fly
            
            rangeEnd = rangeStart + stepSize - 1
            if self.records:
                rangeEnd = min(rangeEnd, self.records)
            elif self.minRecords:
                rangeEnd = min(rangeEnd, self.minRecords)
            res = self.table.insert(r.range(rangeStart, rangeEnd + 1).map({'id':r.row}), conflict='replace').run(self.conn)
            
            assert res['errors'] == 0, 'There were errors inserting id range from %d to %d:\n%s' % (rangeStart, rangeEnd, pprint.pformat(res))
            assert res['unchanged'] == 0, 'There were conflicting records in range from %d to %d:\n%s' % (rangeStart, rangeEnd, pprint.pformat(res))
            assert res['inserted'] == rangeEnd - rangeStart + 1, 'The expected number of rows (%d) were not inserted from id range %d to %d:\n%s' % (rangeEnd - rangeStart + 1, rangeStart, rangeEnd, pprint.pformat(res))
            
            records += res['inserted']
            rangeStart = rangeEnd + 1
        
        # - record the range information
        self.records = records
        self.rangeStart = 1
        self.rangeEnd = rangeEnd
        
        # - rebalance the data across the shards
        self.table.rebalance().run(self.conn) # table wait will happen in caller
        self.__class__.__initalized = True
    
    def _checkData(self, repair=False):
        '''Ensure that the data in the table is as-expected, optionally correcting it. Raises a BadDataException if not.'''
        print('checkData: %s' % self.__class__.__initalized)
        r = self.conn._r
        
        if not self.__class__.__initalized:
            # - handle first-fill
            self._fillInitialData()
        
        elif self.records == 0:
            # - short circut for empty tables
            if repair:
                res = self.table.delete().run(self.conn)
                if res['errors'] != 0:
                    raise BadDataException('Error deleting contents of table:\n%s' % pprint.pformat(res))
            else:
                res = list(self.table.limit(5).run(self.conn))
                if len(res) > 0:
                    raise BadDataException('Extra record%s%s:\n%s' % (
                        's' if len(res) > 1 else '',
                        ', first 5' if len(res) > 4 else '',
                        utils.pformat(res)
                    ))
            
            self.records    = 0
            self.rangeStart = None
            self.rangeEnd   = None
            return
        
        else:
            # -- check/remove out-of-range items
            
            # - before
            if repair:
                res = self.table.between(r.minval, self.rangeStart or r.maxval).delete().run(self.conn)
                if res['errors'] != 0:
                    raise BadDataException('Unable to clear extra records before the range:\n%s' % utils.pformat(res))
            else:
                res = list(self.table.between(r.minval, self.rangeStart or r.maxval).limit(5).run(self.conn))
                if len(res) > 0:
                    raise BadDataException('Extra record%s before range%s:\n%s' % (
                        's' if len(res) > 1 else '',
                        ', first 5' if len(res) > 4 else '',
                        utils.pformat(res)
                    ))
            
            # - after
            if repair:
                res = self.table.between(self.rangeEnd or r.minval, r.maxval, left_bound='open').delete().run(self.conn)
                if res['errors'] != 0:
                    raise BadDataException('Unable to clear extra records after the range:\n%s' % utils.pformat(res))
            else:
                res = list(self.table.between(self.rangeEnd or r.minval, r.maxval, left_bound='open').limit(5).run(self.conn))
                if len(res) > 0:
                    raise BadDataException('Extra record%s after range%s:\n%s' % (
                        's' if len(res) > 1 else '',
                        ', first 5' if len(res) > 4 else '',
                        utils.pformat(res)
                    ))
            
            # -- check/fix in-range records
            
            # - extra records (non-integer ids in range)
            query = self.table.filter(lambda row: row[self.primaryKey].round().ne(row[self.primaryKey]))
            if repair:
                res = query.delete().run(self.conn)
                if res['errors'] != 0:
                    raise BadDataException('Unable to clear extra records in the range:\n%s' % utils.pformat(res))
            else:
                res = list(query.limit(5).run(self.conn))
                if len(res) > 0:
                    raise BadDataException('Extra record%s in range%s:\n%s' % (
                        's' if len(res) > 1 else '',
                        ', first 5' if len(res) > 4 else '',
                        utils.pformat(res)
                    ))
            
            # - extra fields
            query = self.table.filter(lambda row: row.keys().count().ne(1))
            if repair:
                res = query.replace({self.primaryKey:r.row[self.primaryKey]}).run(self.conn)
                if res['errors'] != 0:
                    raise BadDataException('Unable to fix records with extra fields in the range:\n%s' % utils.pformat(res))
            else:
                res = list(query.limit(5).run(self.conn))
                if len(res) > 0:
                    raise BadDataException('Record%s with extra fields in range%s:\n%s' % (
                        's' if len(res) > 1 else '',
                        ', first 5' if len(res) > 4 else '',
                        utils.pformat(res)
                    ))
            
            # -- check/replace any missing records
            
            # - missing records
            batchSize = 1000
            rangeStart = self.rangeStart
            while rangeStart < self.rangeEnd:
                rangeEnd = min(rangeStart + batchSize, self.rangeEnd + 1)
                query = r.range(rangeStart, rangeEnd).coerce_to('array').set_difference(self.table[self.primaryKey].coerce_to('array'))
                if repair:
                    res = query.for_each(self.table.insert({self.primaryKey:r.row}, conflict='replace')).run(self.conn)
                    if res and res['errors'] != 0:
                        raise BadDataException('Unable to fix records with extra fields in the range %d to %d:\n%s' % (rangeStart, rangeEnd, utils.pformat(res)))
                else:
                    res = list(query.limit(5).run(self.conn))
                    if len(res) > 0:
                        raise BadDataException('Missing record%s%s:\n%s' % (
                            's' if len(res) > 1 else '',
                            ', first 5' if len(res) > 4 else '',
                            utils.pformat(res)
                        ))
                rangeStart = rangeEnd
        
        # - rebalance the data across the shards
        if repair:
            self.table.rebalance().run(self.conn) # wait will happen in caller

class TestCaseCompatible(unittest.TestCase):
    '''Replace missing bits from various versions of Python'''
    
    def __init__(self, *args, **kwargs):
        super(TestCaseCompatible, self).__init__(*args, **kwargs)
        if not hasattr(self, 'assertIsNone'):
            self.assertIsNone = self.replacement_assertIsNone
        if not hasattr(self, 'assertIsNotNone'):
            self.assertIsNotNone = self.replacement_assertIsNotNone
        if not hasattr(self, 'assertGreater'):
            self.assertGreater = self.replacement_assertGreater
        if not hasattr(self, 'assertGreaterEqual'):
            self.assertGreaterEqual = self.replacement_assertGreaterEqual
        if not hasattr(self, 'assertLess'):
            self.assertLess = self.replacement_assertLess
        if not hasattr(self, 'assertLessEqual'):
            self.assertLessEqual = self.replacement_assertLessEqual
        if not hasattr(self, 'assertIn'):
            self.assertIn = self.replacement_assertIn
        if not hasattr(self, 'assertRaisesRegexp'):
            self.assertRaisesRegexp = self.replacement_assertRaisesRegexp
        
        if not hasattr(self, 'skipTest'):
            self.skipTest = self.replacement_skipTest
        
    def replacement_assertIsNone(self, val):
        if val is not None:
            raise AssertionError('%s is not None' % val)
    
    def replacement_assertIsNotNone(self, val):
        if val is None:
            raise AssertionError('%s is None' % val)
    
    def replacement_assertGreater(self, actual, expected):
        if not actual > expected:
            raise AssertionError('%s not greater than %s' % (actual, expected))
    
    def replacement_assertGreaterEqual(self, actual, expected):
        if not actual >= expected:
            raise AssertionError('%s not greater than or equal to %s' % (actual, expected))
    
    def replacement_assertLess(self, actual, expected):
        if not actual < expected:
            raise AssertionError('%s not less than %s' % (actual, expected))
    
    def replacement_assertLessEqual(self, actual, expected):
        if not actual <= expected:
            raise AssertionError('%s not less than or equal to %s' % (actual, expected))
    
    def replacement_assertIsNotNone(self, val):
        if val is None:
            raise AssertionError('Result is None')
    
    def replacement_assertIn(self, val, iterable):
        if not val in iterable:
            raise AssertionError('%s is not in %s' % (val, iterable))
    
    def replacement_assertRaisesRegexp(self, exception, regexp, callable_func, *args, **kwds):
        try:
            callable_func(*args, **kwds)
        except Exception as e:
            self.assertTrue(isinstance(e, exception), '%s expected to raise %s but instead raised %s: %s\n%s' % (repr(callable_func), repr(exception), e.__class__.__name__, str(e), traceback.format_exc()))
            self.assertTrue(re.search(regexp, str(e)), '%s did not raise the expected message "%s", but rather: %s' % (repr(callable_func), str(regexp), str(e)))
        else:
            self.fail('%s failed to raise a %s' % (repr(callable_func), repr(exception)))
    
    def replacement_skipTest(self, message):
        sys.stderr.write("%s " % message)

class RdbTestCase(TestCaseCompatible):
    
    # -- settings
    
    servers      = None # defaults to shards * replicas
    tables       = 1 # a number, name, TableManager, or list of names and TableManager's
                     # will be translated into a list of TableManager instances
    
    use_tls      = False
    server_command_prefix = None
    server_extra_options  = None
    
    # - table defaults
    
    primaryKey   = 'id'
    records      = None # if set, minRecords and minFillSecs are ignored 
    minRecords   = None # minimum number of records to create while filling table
    minFillSecs  = None # minimum seconds to fill the table for
    durability   = None
    writeAcks    = None
    shards       = 1
    replicas     = 1
    
    # - general settings
    
    samplesPerShard = 5 # when making changes the number of changes to make per shard
    destructiveTest = False # if true the cluster should be restarted after this test
    
    # -- class variables
    
    dbName            = None # typically 'test'
    tableName         = None # name of the first table
    
    __cluster         = None
    __conn            = None
    __db              = None # r.db(dbName)
    __table           = None # r.db(dbName).table(tableName)
    
    r = utils.import_python_driver()
    
    # -- unittest subclass variables 
    
    __currentResult   = None
    __problemCount    = None
    
    # --
    
    def run(self, result=None):
        # - allow detecting test failure in tearDown
        self.__currentResult = result or self.defaultTestResult()
        self.__problemCount = 0 if result is None else len(self.__currentResult.errors) + len(self.__currentResult.failures)
        
        # - let supper to do its thing
        super(RdbTestCase, self).run(self.__currentResult)
    
    @property
    def cluster(self):
        return self.__cluster
    
    @property
    def db(self):
        if self.__db is None and self.dbName:
            return self.r.db(self.dbName)
        return self.__db
    
    @property
    def table(self):
        if self.__table is None and self.tableName and self.db:
            self.__class__.__table = self.db.table(self.tableName)
        return self.__table
    
    @property
    def tableNames(self):
        return [x.tableName for x in self.tables]
    
    @property
    def conn(self):
        '''Retrieve a valid connection to some server in the cluster'''
        
        return self.conn_function()
    
    @classmethod
    def conn_function(cls, alwaysNew=False):
        '''Retrieve a valid connection to some server in the cluster'''
        
        # -- check if we already have a good cached connection
        if not alwaysNew:
            if cls.__conn and cls.__conn.is_open():
                try:
                    cls.r.expr(1).run(cls.__conn)
                    return cls.__conn
                except Exception: pass
            if cls.conn is not None:
                try:
                    cls.__conn.close()
                except Exception: pass
                cls.__conn = None
        
        # -- try a new connection to each server in order
        for server in cls.__cluster:
            if not server.ready:
                continue
            try:
                ssl = {'ca_certs':self.Cluster.tlsCertPath} if self.use_tls else None
                conn = cls.r.connect(host=server.host, port=server.driver_port, ssl=ssl)
                if not alwaysNew:
                    cls.__conn = conn
                return conn
            except Exception as e: pass
        else:        
            # fail as we have run out of servers
            raise Exception('Unable to get a connection to any server in the cluster')
    
    def getPrimaryForShard(self, index, tableName=None, dbName=None):
        if tableName is None:
            tableName = self.tableName
        if dbName is None:
            dbName = self.dbName
        
        serverName = self.r.db(dbName).table(tableName).config()['shards'].nth(index)['primary_replica'].run(self.conn)
        for server in self.cluster:
            if server.name == serverName:
                return server
        return None
    
    def getReplicasForShard(self, index, tableName=None, dbName=None):
    
        if tableName is None:
            tableName = self.tableName
        if dbName is None:
            dbName = self.dbName
        
        shardsData = self.r.db(dbName).table(tableName).config()['shards'].nth(index).run(self.conn)
        replicaNames = [x for x in shardsData['replicas'] if x != shardsData['primary_replica']]
        
        replicas = []
        for server in self.cluster:
            if server.name in replicaNames:
                replicas.append(server)
        return replicas
    
    def getReplicaForShard(self, index, tableName=None, dbName=None):
        replicas = self.getReplicasForShard(index, tableName=None, dbName=None)
        if replicas:
            return replicas[0]
        else:
            return None
    
    @classmethod
    def checkCluster(cls):
        '''Check that all the servers are running and the cluster is in good shape. Errors on problems'''
        
        assert cls.cluster is not None, 'The cluster was None'
        cls.cluster.check()
        res = list(cls.r.db('rethinkdb').table('current_issues').filter(cls.r.row["type"] != "memory_error").run(cls.conn_function()))
        assert res == [], 'There were unexpected issues: \n%s' % utils.RePrint.pformat(res)
    
    @classmethod
    def setUpClass(cls):
        # -- setup tables for management
        
        defaultDb, defaultTable = utils.get_test_db_table()
        if cls.dbName:
            defaultDb = cls.dbName
        if cls.tableName:
            defaultTable = cls.tableName
        
        # - translate cls.tables into array of TableManager instances
        if cls.tables:
            tables = []
            if isinstance(cls.tables, (int, long)):
                if cls.tables == 1:
                    tables = [defaultTable]
                else:
                    tables = ['%s_%d' % (defaultTable, i) for i in range(1, cls.tables + 1)]
            elif isinstance(cls.tables, (str, unicode)):
                tables = [cls.tables]
            elif hasattr(cls.tables, '__iter__'):
                tables = cls.tables
            else:
                tables = [cls.tables]
            
            for i, table in enumerate(copy.copy(tables)):
                if isinstance(table, (str, unicode)):
                    tables[i] = TableManager(table, defaultDb)
                else:
                    assert isinstance(table, TableManager), 'Items in cls.tables must be names or TableManager instances: %r' % table
            
            for table in tables:
                table.defaultSettings(
                    cls.conn_function,
                    dbName=cls.dbName,
                    records=cls.records, minRecords=cls.minRecords, minFillSecs=cls.minFillSecs,
                    primaryKey=cls.primaryKey, durability=cls.durability, writeAcks=cls.writeAcks,
                    shards=cls.shards, replicas=cls.replicas
                )
            print('ppp', cls)
            cls.tables    = tables
            cls.dbName    = cls.tables[0].dbName
            cls.tableName = cls.tables[0].tableName
        else:
            cls.tables    = []
            cls.dbName    = defaultDb
            cls.tableName = defaultTable
        
        cls.__db      = cls.r.db(cls.dbName)
        cls.__table   = cls.__db.table(cls.tableName)
    
    @classmethod
    def setUpCluster(cls):
        # - check on an existing cluster
        if cls.cluster is not None:
            try:
                cls.checkCluster()
            except:
                try:
                    cls.cluster.check_and_stop()
                except Exception: pass
                cls.__cluster = None
                cls.__conn    = None
        
        # - ensure we have a cluster
        if cls.__cluster is None:
            cls.__cluster = driver.Cluster(tls=cls.use_tls)
        
        # - make sure we have any named servers
        servers = [cls.servers] if isinstance(cls.servers, (str, unicode)) else (cls.servers or 0)
        if hasattr(servers, '__iter__'):
            for name in servers:
                firstServer = len(cls.__cluster) == 0
                if not name in cls.__cluster:
                    driver.Process(cluster=cls.__cluster, name=name, console_output=True, command_prefix=cls.server_command_prefix, extra_options=cls.server_extra_options, wait_until_ready=firstServer)
        
        # - ensure we have the proper number of servers: enough that each server has only one role
        serverCount = max(cls.shards * cls.replicas, len(servers) if hasattr(servers, '__iter__') else servers)
        for _ in range(serverCount - len(cls.__cluster)):
            firstServer = len(cls.__cluster) == 0
            driver.Process(cluster=cls.__cluster, console_output=True, command_prefix=cls.server_command_prefix, extra_options=cls.server_extra_options, wait_until_ready=firstServer)
        
        cls.__cluster.wait_until_ready()
    
    def setUp(self):
        # - run setUpClass if not run otherwise (fix for Python2.6)
        if not hasattr(unittest.TestCase, 'setUpClass') and hasattr(self.__class__, 'setUpClass') and not hasattr(self.__class__, self.__class__.__name__ + '_setup'):
            self.setUpClass()
            setattr(self.__class__, self.__class__.__name__ + '_setup', True)
        
        # - make sure the servers are running
        self.setUpCluster()
                
        # - ensure db is available
        if self.dbName:
            self.r.expr([self.dbName]).set_difference(self.r.db_list()).for_each(self.r.db_create(self.r.row)).run(self.conn)        
        
        # - ensure managed tables are in the right state
        print('setUp: %s.%s: %s' % (self.dbName, self.tableName, [(x.tableName, repr(x)) for x in self.tables]))
        for table in self.tables:
            table.check(repair=True)
    
    def tearDown(self):
        # -- verify that the servers are still running
        
        lastError = None
        for server in self.cluster:
            if server.running is False:
                continue
            try:
                server.check()
            except Exception as e:
                lastError = e
        
        # -- check that there were not problems in this test
        
        allGood = self.__problemCount == len(self.__currentResult.errors) + len(self.__currentResult.failures)
        
        if lastError is not None or not allGood:
            # - stop all of the servers
            try:
                self.cluster.check_and_stop()
            except Exception: pass
            
            # - save the server data
            try:
                # - create enclosing dir
                name = self.id()
                if name.startswith('__main__.'):
                    name = name[len('__main__.'):]
                outputFolder = os.path.realpath(os.path.join(os.getcwd(), name))
                if not os.path.isdir(outputFolder):
                    os.makedirs(outputFolder)
                
                # - copy the servers data
                for server in self.cluster:
                    shutil.copytree(server.data_path, os.path.join(outputFolder, os.path.basename(server.data_path)))
            
            except Exception as e:
                warnings.warn('Unable to copy server folder into results: %s' % str(e))
            
            # - clear internal values
            self.__class__.__cluster = None
            self.__class__.__conn    = None
            if lastError:
                raise lastError
        
        elif self.destructiveTest:
            try:
                self.cluster.check_and_stop()
            except Exception: pass
            self.__class__.__cluster = None
            self.__class__.__conn    = None
    
    def makeChanges(self, tableName=None, dbName=None, samplesPerShard=None, connections=None):
        '''make a minor change to records, and return those ids'''
        
        if tableName is None:
            tableName = self.tableName
        if dbName is None:
            dbName = self.dbName
        
        if samplesPerShard is None:
            samplesPerShard = self.samplesPerShard
        
        if connections is None:
            connections = itertools.cycle([self.conn])
        else:
            connections = itertools.cycle(connections)
        
        changedRecordIds = []
        for lower, upper in utils.getShardRanges(connections.next(), tableName):
            
            conn = connections.next()
            sampleIds = (x['id'] for x in self.r.db(dbName).table(tableName).between(lower, upper).sample(samplesPerShard).run(conn))
            
            for thisId in sampleIds:
                self.r.db(dbName).table(tableName).get(thisId).update({'randomChange':random.randint(0, 65536)}).run(conn)
                changedRecordIds.append(thisId)
        
        changedRecordIds.sort()
        return changedRecordIds

# == internal testing

if __name__ == '__main__':

    class TableManager_empty_Test(RdbTestCase):
        records = 0
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
            self.assertEqual(self.table.count().run(self.conn), 0)
    
    class TableManager_multiple_tables_Test(RdbTestCase):
        tables     = 2
        records    = 4
        
        def test_setup(self):
            tableNames = list(self.db.table_list().run(self.conn))
            self.assertTrue(self.tableName in tableNames, 'Did not find < %s > in list: %s' % (self.tableName, tableNames))
            self.assertEqual(len(tableNames), 2)
            
            for thisOne in self.tables:
                actualCount = thisOne.table.count().run(self.conn)
                self.assertEqual(actualCount, thisOne.records)
                self.assertEqual(actualCount, self.records)
    
    class TableManager_minRecords_Test(RdbTestCase):
        tables     = 1
        minRecords = 10
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
            self.assertEqual(len(self.tables), 1)
            
            actualCount = self.table.count().run(self.conn)
            self.assertGreaterEqual(self.records, self.minRecords)
            self.assertEqual(actualCount, self.tables[0].records)
            self.assertTrue(actualCount >= self.minRecords, 'Too few records, actual: %r vs. expected min: %r' % (actualCount, self.minRecords))
        
        def test__checkData(self):
            # - grab the number of records as a check
            intialRecords = self.table.count().run(self.conn)
            
            # - delete one record, add one record in range and one out-of-range, change one record
            self.table.get(1).delete().run(self.conn)
            self.table.insert([{'id':1.5}, {}]).run(self.conn)
            self.table.get(2).update({'extra':'bit'}).run(self.conn)
            
            # - confirm we error
            self.assertRaises(BadDataException, self.tables[0]._checkData)
            
            # - confirm we can fix it
            self.tables[0]._checkData(repair=True)
            actualRecords = self.table.count().run(self.conn)
            self.assertEqual(actualRecords, intialRecords, 'After checking the data, did not have the right number of records: %d vs. expected: %d' % (actualRecords, intialRecords))
    
    class TableManager_minFillSecs_Test(RdbTestCase):
        minFillSecs = 1.5
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
            self.assertEqual(len(self.tables), 1)
            
            actualCount = self.table.count().run(self.conn)
            self.assertEqual(actualCount, self.tables[0].records)
            self.assertTrue(actualCount > 0, 'No records in the table')
        
        def test__checkData(self):
            # - grab the number of records as a check
            initialRecords = self.table.count().run(self.conn)
            
            # - delete one record, add one record in range and one out-of-range, change one record
            self.table.get(1).delete().run(self.conn)
            self.table.insert([{'id':1.5}, {}]).run(self.conn)
            self.table.get(2).update({'extra':'bit'}).run(self.conn)
            
            # - confirm we error
            self.assertRaises(BadDataException, self.tables[0]._checkData)
            
            # - confirm we can fix it
            self.tables[0]._checkData(repair=True)
            actualRecords = self.table.count().run(self.conn)
            self.assertEqual(actualRecords, initialRecords, 'After checking the data, did not have the right number of records: %d vs. expected: %d' % (actualRecords, initialRecords))
        
    class TableManager_records_Test(RdbTestCase):
        records = 302
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
            self.assertEqual(len(self.tables), 1)
            
            actualCount = self.table.count().run(self.conn)
            self.assertEqual(self.tables[0].records, self.records)
            self.assertEqual(actualCount, self.records, 'Incorrect number of records, actual: %r vs. expected %d' % (actualCount, self.records))
        
        def test__checkData(self):
            # - grab the number of records as a check
            initialRecords = self.table.count().run(self.conn)
            self.assertEqual(initialRecords, self.records)
            
            # - delete one record, add one record in range and one out-of-range, change one record
            self.table.get(1).delete().run(self.conn)
            self.table.insert([{'id':1.5}, {}]).run(self.conn)
            self.table.get(2).update({'extra':'bit'}).run(self.conn)
            
            # - confirm we error
            self.assertRaises(BadDataException, self.tables[0]._checkData)
            
            # - confirm we can fix it
            self.tables[0]._checkData(repair=True)
            actualRecords = self.table.count().run(self.conn)
            self.assertEqual(actualRecords, self.records, 'After checking the data, did not have the right number of records: %d vs. expected: %d' % (actualRecords, self.records))
    
    main()

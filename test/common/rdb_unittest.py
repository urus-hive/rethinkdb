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

class EmptyTableManager():
    '''Manages a single table to allow for clean re-use between tests'''
    
    # - settings
    
    tableName = None
    dbName = None
    
    primaryKey = None
    shards = 1
    replicas = 1
    durability = 'hard'
    writeAcks = 'majority'
    
    minRecords = None  # minimum number of records to fill with
    minFillSecs = None # minumim seconds to fill
    
    # - running variables
    
    records = None     # if set, rangeStart and rangeEnd are ignored 
    rangeStart = None  # first key in the range of data
    rangeEnd = None    # last key in the range of data
    
    # - internal cache values
    
    _conn = None
    _table = None
    _saved_config = None
    
    # --
    
    def __init__(self, tableName, dbName, conn, records=None, minRecords=None, minFillSecs=None, primaryKey=None, durability=None, writeAcks=None):
        
        # -- initial values
        
        # - table
        assert tableName is not None, 'tableName value required (got None)'
        self.tableName = str(tableName)
        
        # - dbName
        assert dbName is not None, 'dbName value required (got None)'
        self.dbName = str(dbName)
        
        # - conn
        assert conn is not None, 'conn value required (got None)'
        if hasattr(conn, 'reconnect'):
            self._conn = conn
        elif hasattr(conn, '__call__') and hasattr(conn(), 'reconnect'):
            self._conn = conn
        else:
            raise ValueError('Bad conn value: %r' % conn)
        
        # - records/minRecords/minFillSecs
        
        if records is not None:
            try:
                self.records = int(records)
                assert self.records > 0
            except Exception:
                raise ValueError('GBad minRecords value: %r' % records)
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
        self.primaryKey = primaryKey or 'id'
        
        # - durability
        if durability is not None:
            self.durability = str(durability)
        
        # - writeAcks
        if writeAcks is not None:
            self.writeAcks = str(writeAcks)
        
        # -- inital table creation/fill
        
        self._checkTable(repair=True)
        self._fillInitialData()
    
    @property
    def conn(self):
        if self._conn is None:
            raise Exception('conn is not defined')
        elif hasattr(self._conn, '__call__'):
            return self._conn()
        else:
            return self._conn
    
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
                self._fillInitialData()
        self.table.wait().run(self.conn)
    
    def _checkTable(self, repair=False):
        '''Ensures that the table is in place with the correct data and settings'''
        
        r = self.conn._r
        
        forceRedo = repair == 'force'
        
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
            
            # - forceRedo
            if forceRedo:
                r.expr([self.tableName]).set_difference(r.db(self.dbName).table_list()).for_each(r.db(self.dbName).table_delete(r.row)).run(self.conn)
            
            # - table/primary key
            primaryKeys = r.db('rethinkdb').table('table_config').filter({'db':self.dbName, 'name':self.tableName}).pluck('primary_key')['primary_key'].coerce_to('array').run(self.conn)
                
            if primaryKeys != [self.primaryKey]:
                # bad primary key, drop the table if it exists and create it with the proper key
                if len(primaryKeys) == 1:
                    self.conn._r.db(self.dbName).table_drop(self.tableName).run(self.conn)
                elif len(primaryKeys) > 1:
                    raise BadTableException('Somehow there was were multiple tables named %s.%s' %  (self.dbName, self.tableName))
                r.db(self.dbName).table_create(self.tableName, primary_key=self.primaryKey).run(self.conn)
                self.table.wait().run(self.conn)
            
            # - remove secondary indexes - todo: make this actually be able to reset indexes
            self.table.index_status().pluck('index')['index'].for_each(self.table.index_drop(self.conn._r.row)).run(self.conn)
            
            # - durability/writeAcks
            configInfo = self.table.config().run(self.conn)
            if configInfo['durability'] != self.durability or configInfo['write_acks'] != self.writeAcks:
                res = self.table.config().update({'durability':self.durability, 'write_acks':self.writeAcks}).run(self.conn)
                if res['errors'] != 0:
                    raise BadTableException('Failed updating table metadata: %s' % str(res))
            
            # - sharding/replication
            shardInfo = configInfo['shards']
            if len(shardInfo) != self.shards or not all([len(x['replicas']) == self.replicas for x in shardInfo]):
                if len(self.cluster) < self.shards * self.replicas:
                    raise BadTableException('Cluster does not have enough servers to only put one shard on each: %d vs %d * %d' % (len(self.cluster), self.shards, self.replicas))
                
                replicas = iter(self.cluster[self.shards:])
                shardPlan = []
                for primary in self.cluster[:self.shards]:
                    chosenReplicas = [replicas.next().name for _ in range(0, self.replicas - 1)]
                    shardPlan.append({'primary_replica':primary.name, 'replicas':[primary.name] + chosenReplicas})
                
                res = self.r.db(self.dbName).table(self.tableName).config().update({'shards':shardPlan}).run(self.conn)
                if res['errors'] != 0:
                    raise BadTableException('Failed updating shards: %s' % str(res))
        
        # -- wait for table to be ready
        
        self.table.wait().run(self.conn)
    
    def _fillInitialData(self):
        '''Insert inital data, for the base class this is nothing'''
        pass
    
    def _checkData(self, repair=False):
        '''Ensure that the table is empty, optionally deleting data to get there'''
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
                    pprint.pformat(res)
                ))
        
        self.records = 0
        self.rangeStart = None
        self.rangeEnd = None

class SimpleTableManager(EmptyTableManager):
    '''Records look like: { self.primaryKey: integer }'''
    
    def _fillInitialData(self, minRecords=None, minFillSecs=None):
        '''Fill the table with the initial set of data, and record the number of records'''
        
        r = self.conn._r
        
        # - handle pre-created tables
        
        existingRecords = self.table.count().run(self.conn)
        if existingRecords:
            if self.records:
                self.rangeStart = 1
                self.rangeEnd = self.rangeStart + self.records - 1
                self._checkData(repair=True)
                return
            elif self.minRecords and self.minFillSecs is None and self.minRecords < existingRecords:
                self.records = existingRecords
                self._checkData(repair=True)
                return
            self.table.delete().run(self.conn)
        
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
    
    def _checkData(self, repair=False):
        '''Ensure that the data in the table is as-expected, optionally correcting it. Raises a BadDataException if not.'''
        
        r = self.conn._r
        
        # -- check/remove out-of-range items
        
        # - before
        if repair:
            res = self.table.between(r.minval, self.rangeStart).delete().run(self.conn)
            if res['errors'] != 0:
                raise BadDataException('Unable to clear extra records before the range:\n%s' % pprint.pformat(res))
        else:
            res = list(self.table.between(r.minval, self.rangeStart).limit(5).run(self.conn))
            if len(res) > 0:
                raise BadDataException('Extra record%s before range%s:\n%s' % (
                    's' if len(res) > 1 else '',
                    ', first 5' if len(res) > 4 else '',
                    pprint.pformat(res)
                ))
        
        # - after
        if repair:
            res = self.table.between(self.rangeEnd, r.maxval, left_bound='open').delete().run(self.conn)
            if res['errors'] != 0:
                raise BadDataException('Unable to clear extra records after the range:\n%s' % pprint.pformat(res))
        else:
            res = list(self.table.between(self.rangeEnd, r.maxval, left_bound='open').limit(5).run(self.conn))
            if len(res) > 0:
                raise BadDataException('Extra record%s after range%s:\n%s' % (
                    's' if len(res) > 1 else '',
                    ', first 5' if len(res) > 4 else '',
                    pprint.pformat(res)
                ))
        
        # -- check/fix in-range records
        
        # - extra records (non-integer ids in range)
        query = self.table.filter(lambda row: row[self.primaryKey].round().ne(row[self.primaryKey]))
        if repair:
            res = query.delete().run(self.conn)
            if res['errors'] != 0:
                raise BadDataException('Unable to clear extra records in the range:\n%s' % pprint.pformat(res))
        else:
            res = list(query.limit(5).run(self.conn))
            if len(res) > 0:
                raise BadDataException('Extra record%s in range%s:\n%s' % (
                    's' if len(res) > 1 else '',
                    ', first 5' if len(res) > 4 else '',
                    pprint.pformat(res)
                ))
        
        # - extra fields
        query = self.table.filter(lambda row: row.keys().count().ne(1))
        if repair:
            res = query.replace({self.primaryKey:r.row[self.primaryKey]}).run(self.conn)
            if res['errors'] != 0:
                raise BadDataException('Unable to fix records with extra fields in the range:\n%s' % pprint.pformat(res))
        else:
            res = list(query.limit(5).run(self.conn))
            if len(res) > 0:
                raise BadDataException('Record%s with extra fields in range%s:\n%s' % (
                    's' if len(res) > 1 else '',
                    ', first 5' if len(res) > 4 else '',
                    pprint.pformat(res)
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
                    raise BadDataException('Unable to fix records with extra fields in the range %d to %d:\n%s' % (rangeStart, rangeEnd, pprint.pformat(res)))
            else:
                res = list(query.limit(5).run(self.conn))
                if len(res) > 0:
                    raise BadDataException('Missing record%s%s:\n%s' % (
                        's' if len(res) > 1 else '',
                        ', first 5' if len(res) > 4 else '',
                        pprint.pformat(res)
                    ))
            rangeStart = rangeEnd

class RdbTestCase(unittest.TestCase):
    
    # -- settings
    
    servers = None # defaults to shards * replicas
    server_command_prefix = None
    server_extra_options = None
    
    # - main table settings
    
    tableManager = EmptyTableManager # set as TableManager class, not instance

    shards = 1
    replicas = 1
    tables   = 1 # either a number, a name, or a list of names
    
    primaryKey   = 'id'
    records      = None
    minRecords   = None
    minFillSecs  = None
    durability   = None
    writeAcks    = None
    
    # - general settings
    
    samplesPerShard = 5 # when making changes the number of changes to make per shard
    
    destructiveTest = False # if true the cluster should be restarted after this test
    
    # -- class variables
    
    dbName = None
    tableName = None
    
    db = None
    table = None
    
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
        if self.tables:
            defaultDb, defaultTable = utils.get_test_db_table()
            
            if not self.dbName:
                self.dbName = defaultDb
            
            if isinstance(self.tables, (int, long)):
                if self.tables == 1:
                    self.tableNames = [defaultTable]
                else:
                    self.tableNames = ['%s_%d' % (defaultTable, i) for i in range(1, self.tables + 1)]
            elif isinstance(self.tables, (str, unicode)):
                self.tableNames = [self.tables]
            elif hasattr(self.tables, '__iter__'):
                self.tableNames = [str(x) for x in self.tables]
            else:
                raise Exception('The value of tables was not recogised: %r' % self.tables)
            
            self.tableName = self.tableNames[0]
        
        # -- set db and table
        self.__class__.db = self.r.db(self.dbName)
        self.__class__.table = self.db.table(self.tableName)
        
        # -- Allow detecting test failure in tearDown
        self.__currentResult = result or self.defaultTestResult()
        self.__problemCount = 0 if result is None else len(self.__currentResult.errors) + len(self.__currentResult.failures)
        
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
    def conn(self):
        '''Retrieve a valid connection to some server in the cluster'''
        
        # -- check if we already have a good cached connection
        if self.__class__.__conn and self.__class__.__conn.is_open():
            try:
                self.r.expr(1).run(self.__class__.__conn)
                return self.__class__.__conn
            except Exception: pass
        if self.__class__.conn is not None:
            try:
                self.__class__.__conn.close()
            except Exception: pass
            self.__class__.__conn = None
        
        # -- try a new connection to each server in order
        for server in self.cluster:
            if not server.ready:
                continue
            try:
                ssl = {'ca_certs':self.Cluster.tlsCertPath} if self.use_tls else None
                self.__class__.__conn = self.r.connect(host=server.host, port=server.driver_port, ssl=ssl)
                return self.__class__.__conn
            except Exception as e: pass
        else:        
            # fail as we have run out of servers
            raise Exception('Unable to get a connection to any server in the cluster')
    
    def conn_function(self):
        return self.conn
    
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
    
    def checkCluster(self):
        '''Check that all the servers are running and the cluster is in good shape. Errors on problems'''
        
        assert self.cluster is not None, 'The cluster was None'
        self.cluster.check()
        res = list(self.r.db('rethinkdb').table('current_issues').filter(self.r.row["type"] != "memory_error").run(self.conn))
        assert res == [], 'There were unexpected issues: \n%s' % utils.RePrint.pformat(res)
    
    def setUp(self):
        
        # -- start the servers
        
        # - check on an existing cluster
        
        if self.cluster is not None:
            try:
                self.checkCluster()
            except:
                try:
                    self.cluster.check_and_stop()
                except Exception: pass
                self.__class__.__cluster = None
                self.__class__.__conn    = None
                self.__class__.__table   = None
        
        # - ensure we have a cluster
        
        if not isinstance(self.cluster, driver.Cluster):
            self.__class__.cluster = driver.Cluster()
        
        # - make sure we have any named servers
        
        if hasattr(self.servers, '__iter__'):
            for name in self.servers:
                firstServer = len(self.cluster) == 0
                if not name in self.cluster:
                    driver.Process(cluster=self.cluster, name=name, console_output=True, command_prefix=self.server_command_prefix, extra_options=self.server_extra_options, wait_until_ready=firstServer)
        
        # - ensure we have the proper number of servers
        # note: we start up enough servers to make sure they each have only one role
        
        serverCount = max(self.shards * self.replicas, len(self.servers) if hasattr(self.servers, '__iter__') else self.servers)
        for _ in range(serverCount - len(self.cluster)):
            firstServer = len(self.cluster) == 0
            driver.Process(cluster=self.cluster, console_output=True, command_prefix=self.server_command_prefix, extra_options=self.server_extra_options, wait_until_ready=firstServer)
        
        self.cluster.wait_until_ready()
        
        # -- setup managed table if present
        
        if inspect.isclass(self.__class__.tableManager) and issubclass(self.__class__.tableManager, EmptyTableManager):
            self.__class__.tableManager = self.__class__.tableManager(tableName=self.tableName, dbName=self.dbName, conn=self.conn_function, records=self.records, minRecords=self.minRecords, minFillSecs=self.minFillSecs, primaryKey=self.primaryKey, durability=None, writeAcks=self.writeAcks)
        
        # -- ensure db is available
        
        self.r.expr([self.dbName]).set_difference(self.r.db_list()).for_each(self.r.db_create(self.r.row)).run(self.conn)
        
        # -- setup test table
        
        if self.tableName is not None:
            
            if isinstance(self.tableManager, EmptyTableManager):
                self.tableManager.check()
            else:
                # - ensure an empty table
                self.conn._r.expr([self.tableName]).set_difference(self.db.table_list()).for_each(self.db.table_delete(self.conn._r.row)).run(self.conn)
                self.db.table_create(self.tableName).run(self.conn)
                
                # - shard and replicate the table
                
                primaries = iter(self.cluster[:self.shards])
                replicas = iter(self.cluster[self.shards:])
                
                shardPlan = []
                for primary in primaries:
                    chosenReplicas = [replicas.next().name for _ in range(0, self.replicas - 1)]
                    shardPlan.append({'primary_replica':primary.name, 'replicas':[primary.name] + chosenReplicas})
                res = self.r.db(self.dbName).table(self.tableName).config().update({'shards':shardPlan}).run(self.conn)
                assert res['errors'] == 0, 'Unable to apply shard plan:\n%s' % pprint.pformat(res)
        
        self.table.wait().run(self.conn)
    
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
            
            # -- stop all of the servers
            
            try:
                self.cluster.check_and_stop()
            except Exception: pass
            
            # -- save the server data
                
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
            
            self.__class__.cluster = None
            self.__class__._conn = None
            self.__class__.table = None
            if lastError:
                raise lastError
        
        elif self.destructiveTest:
            try:
                self.cluster.check_and_stop()
            except Exception: pass
            self.__class__.cluster = None
            self.__class__._conn = None
            self.__class__.table = None
    
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

    class EmptyTableManager_Test(RdbTestCase):
        tableManager = EmptyTableManager
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
    
    class SimpleTableManager_minRecords_Test(RdbTestCase):
        tableManager = SimpleTableManager
        minRecords = 10
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
            actualCount = self.table.count().run(self.conn)
            self.assertEqual(actualCount, self.tableManager.records)
            self.assertTrue(actualCount >= self.minRecords, 'To few records, actual: %r vs. expected min: %r' % (actualCount, self.minRecords))
        
        def test__checkData(self):
            # - grab the number of records as a check
            intialRecords = self.table.count().run(self.conn)
            
            # - delete one record, add one record in range and one out-of-range, change one record
            self.table.get(1).delete().run(self.conn)
            self.table.insert([{'id':1.5}, {}]).run(self.conn)
            self.table.get(2).update({'extra':'bit'}).run(self.conn)
            
            # - confirm we error
            self.assertRaises(BadDataException, self.tableManager._checkData)
            
            # - confirm we can fix it
            self.tableManager._checkData(repair=True)
            actualRecords = self.table.count().run(self.conn)
            self.assertEqual(actualRecords, intialRecords, 'After checking the data, did not have the right number of records: %d vs. expected: %d' % (actualRecords, intialRecords))
    
    class SimpleTableManager_minFillSecs_Test(RdbTestCase):
        tableManager = SimpleTableManager
        minFillSecs = 1.5
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
            actualCount = self.table.count().run(self.conn)
            self.assertEqual(actualCount, self.tableManager.records)
            self.assertTrue(actualCount > 0, 'No records in the table')
        
        def test__checkData(self):
            # - grab the number of records as a check
            initialRecords = self.table.count().run(self.conn)
            
            # - delete one record, add one record in range and one out-of-range, change one record
            self.table.get(1).delete().run(self.conn)
            self.table.insert([{'id':1.5}, {}]).run(self.conn)
            self.table.get(2).update({'extra':'bit'}).run(self.conn)
            
            # - confirm we error
            self.assertRaises(BadDataException, self.tableManager._checkData)
            
            # - confirm we can fix it
            self.tableManager._checkData(repair=True)
            actualRecords = self.table.count().run(self.conn)
            self.assertEqual(actualRecords, initialRecords, 'After checking the data, did not have the right number of records: %d vs. expected: %d' % (actualRecords, initialRecords))
        
    class SimpleTableManager_records_Test(RdbTestCase):
        tableManager = SimpleTableManager
        records = 302
        
        def test_setup(self):
            self.assertEqual(self.db.table_list().run(self.conn), [self.tableName])
            actualCount = self.table.count().run(self.conn)
            self.assertEqual(self.tableManager.records, self.records)
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
            self.assertRaises(BadDataException, self.tableManager._checkData)
            
            # - confirm we can fix it
            self.tableManager._checkData(repair=True)
            actualRecords = self.table.count().run(self.conn)
            self.assertEqual(actualRecords, self.records, 'After checking the data, did not have the right number of records: %d vs. expected: %d' % (actualRecords, self.records))
    
    main()

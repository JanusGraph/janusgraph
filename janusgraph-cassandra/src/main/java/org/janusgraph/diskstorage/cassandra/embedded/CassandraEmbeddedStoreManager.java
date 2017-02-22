// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.cassandra.embedded;

import static org.janusgraph.diskstorage.cassandra.CassandraTransaction.getTx;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeoutException;

import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.cassandra.utils.CassandraHelper;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.*;
import org.janusgraph.diskstorage.util.ByteBufferUtil;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.cassandra.cache.CachingOptions;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.composites.CellName;
import org.apache.cassandra.db.composites.CellNames;
import org.apache.cassandra.db.filter.NamesQueryFilter;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.io.compress.CompressionParameters;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.janusgraph.diskstorage.cassandra.utils.CassandraDaemonWrapper;

public class CassandraEmbeddedStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(CassandraEmbeddedStoreManager.class);

    /**
     * The default value for
     * {@link GraphDatabaseConfiguration#STORAGE_CONF_FILE}.
     */
    public static final String CASSANDRA_YAML_DEFAULT = "./conf/cassandra.yaml";

    private final Map<String, CassandraEmbeddedKeyColumnValueStore> openStores;

    private final IRequestScheduler requestScheduler;

    public CassandraEmbeddedStoreManager(Configuration config) throws BackendException {
        super(config);

        String cassandraConfig = CASSANDRA_YAML_DEFAULT;
        if (config.has(GraphDatabaseConfiguration.STORAGE_CONF_FILE)) {
            cassandraConfig = config.get(GraphDatabaseConfiguration.STORAGE_CONF_FILE);
        }

        assert cassandraConfig != null && !cassandraConfig.isEmpty();

        File ccf = new File(cassandraConfig);

        if (ccf.exists() && ccf.isAbsolute()) {
            cassandraConfig = "file://" + cassandraConfig;
            log.debug("Set cassandra config string \"{}\"", cassandraConfig);
        }

        CassandraDaemonWrapper.start(cassandraConfig);

        this.openStores = new HashMap<String, CassandraEmbeddedKeyColumnValueStore>(8);
        this.requestScheduler = DatabaseDescriptor.getRequestScheduler();
    }

    @Override
    public Deployment getDeployment() {
        return Deployment.EMBEDDED;
    }

    @Override
    public IPartitioner getCassandraPartitioner()
            throws BackendException {
        try {
            return StorageService.getPartitioner();
        } catch (Exception e) {
            log.warn("Could not read local token range: {}", e);
            throw new PermanentBackendException("Could not read partitioner information on cluster", e);
        }
    }

    @Override
    public String toString() {
        return "embeddedCassandra" + super.toString();
    }

    @Override
    public void close() {
        openStores.clear();
        CassandraDaemonWrapper.stop();
    }

    @Override
    public synchronized KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        if (openStores.containsKey(name))
            return openStores.get(name);

        // Ensure that both the keyspace and column family exist
        ensureKeyspaceExists(keySpaceName);
        ensureColumnFamilyExists(keySpaceName, name);

        CassandraEmbeddedKeyColumnValueStore store = new CassandraEmbeddedKeyColumnValueStore(keySpaceName, name, this);
        openStores.put(name, store);
        return store;
    }

    /*
     * Raw type warnings are suppressed in this method because
     * {@link StorageService#getLocalPrimaryRanges(String)} returns a raw
     * (unparameterized) type.
     */
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        ensureKeyspaceExists(keySpaceName);

        
        Collection<Range<Token>> ranges = StorageService.instance.getPrimaryRanges(keySpaceName);

        List<KeyRange> keyRanges = new ArrayList<KeyRange>(ranges.size());

        for (Range<Token> range : ranges) {
            keyRanges.add(CassandraHelper.transformRange(range));
        }

        return keyRanges;
    }

    /*
      * This implementation can't handle counter columns.
      *
      * The private method internal_batch_mutate in CassandraServer as of 1.2.0
      * provided most of the following method after transaction handling.
      */
    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(mutations);

        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        int size = 0;
        for (Map<StaticBuffer, KCVMutation> mutation : mutations.values()) size += mutation.size();
        Map<StaticBuffer, org.apache.cassandra.db.Mutation> rowMutations = new HashMap<>(size);

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> mutEntry : mutations.entrySet()) {
            String columnFamily = mutEntry.getKey();
            for (Map.Entry<StaticBuffer, KCVMutation> janusgraphMutation : mutEntry.getValue().entrySet()) {
                StaticBuffer key = janusgraphMutation.getKey();
                KCVMutation mut = janusgraphMutation.getValue();

                org.apache.cassandra.db.Mutation rm = rowMutations.get(key);
                if (rm == null) {
                    rm = new org.apache.cassandra.db.Mutation(keySpaceName, key.asByteBuffer());
                    rowMutations.put(key, rm);
                }

                if (mut.hasAdditions()) {
                    for (Entry e : mut.getAdditions()) {
                        Integer ttl = (Integer) e.getMetaData().get(EntryMetaData.TTL);

                        if (null != ttl && ttl > 0) {
                            rm.add(columnFamily, CellNames.simpleDense(e.getColumnAs(StaticBuffer.BB_FACTORY)),
                                    e.getValueAs(StaticBuffer.BB_FACTORY), commitTime.getAdditionTime(times), ttl);
                        } else {
                            rm.add(columnFamily, CellNames.simpleDense(e.getColumnAs(StaticBuffer.BB_FACTORY)),
                                    e.getValueAs(StaticBuffer.BB_FACTORY), commitTime.getAdditionTime(times));
                        }
                    }
                }

                if (mut.hasDeletions()) {
                    for (StaticBuffer col : mut.getDeletions()) {
                        rm.delete(columnFamily, CellNames.simpleDense(col.as(StaticBuffer.BB_FACTORY)),
                                commitTime.getDeletionTime(times));
                    }
                }

            }
        }

        mutate(new ArrayList<org.apache.cassandra.db.Mutation>(rowMutations.values()), getTx(txh).getWriteConsistencyLevel().getDB());

        sleepAfterWrite(txh, commitTime);
    }

    private void mutate(List<org.apache.cassandra.db.Mutation> cmds, org.apache.cassandra.db.ConsistencyLevel clvl) throws BackendException {
        try {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try {
                if (atomicBatch) {
                    StorageProxy.mutateAtomically(cmds, clvl);
                } else {
                    StorageProxy.mutate(cmds, clvl);
                }
            } catch (RequestExecutionException e) {
                throw new TemporaryBackendException(e);
            } finally {
                release();
            }
        } catch (TimeoutException ex) {
            log.debug("Cassandra TimeoutException", ex);
            throw new TemporaryBackendException(ex);
        }
    }

    private void schedule(long timeoutMS) throws TimeoutException {
        requestScheduler.queue(Thread.currentThread(), "default", DatabaseDescriptor.getRpcTimeout());
    }

    /**
     * Release count for the used up resources
     */
    private void release() {
        requestScheduler.release();
    }

    @Override
    public void clearStorage() throws BackendException {
        openStores.clear();
        try {
            KSMetaData ksMetaData = Schema.instance.getKSMetaData(keySpaceName);

            // Not a big deal if Keyspace doesn't not exist (dropped manually by user or tests).
            // This is called on per test setup basis to make sure that previous test cleaned
            // everything up, so first invocation would always fail as Keyspace doesn't yet exist.
            if (ksMetaData == null)
                return;

            for (String cfName : ksMetaData.cfMetaData().keySet())
                StorageService.instance.truncate(keySpaceName, cfName);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    private void ensureKeyspaceExists(String keyspaceName) throws BackendException {

        if (null != Schema.instance.getKeyspaceInstance(keyspaceName))
            return;

        // Keyspace not found; create it
        String strategyName = storageConfig.get(REPLICATION_STRATEGY);

        KSMetaData ksm;
        try {
            ksm = KSMetaData.newKeyspace(keyspaceName, strategyName, strategyOptions, true);
        } catch (ConfigurationException e) {
            throw new PermanentBackendException("Failed to instantiate keyspace metadata for " + keyspaceName, e);
        }
        try {
            MigrationManager.announceNewKeyspace(ksm);
            log.info("Created keyspace {}", keyspaceName);
        } catch (ConfigurationException e) {
            throw new PermanentBackendException("Failed to create keyspace " + keyspaceName, e);
        }
    }

    private void ensureColumnFamilyExists(String ksName, String cfName) throws BackendException {
        ensureColumnFamilyExists(ksName, cfName, BytesType.instance);
    }

    private void ensureColumnFamilyExists(String keyspaceName, String columnfamilyName, AbstractType<?> comparator) throws BackendException {
        if (null != Schema.instance.getCFMetaData(keyspaceName, columnfamilyName))
            return;

        // Column Family not found; create it
        CFMetaData cfm = new CFMetaData(keyspaceName, columnfamilyName, ColumnFamilyType.Standard, CellNames.fromAbstractType(comparator, true));
        try {
            if (storageConfig.has(COMPACTION_STRATEGY)) {
                cfm.compactionStrategyClass(CFMetaData.createCompactionStrategy(storageConfig.get(COMPACTION_STRATEGY)));
            }
            if (!compactionOptions.isEmpty()) {
                cfm.compactionStrategyOptions(compactionOptions);
            }
        } catch (ConfigurationException e) {
            throw new PermanentBackendException("Failed to create column family metadata for " + keyspaceName + ":" + columnfamilyName, e);
        }

        // Hard-coded caching settings
        if (columnfamilyName.startsWith(Backend.EDGESTORE_NAME)) {
            cfm.caching(CachingOptions.KEYS_ONLY);
        } else if (columnfamilyName.startsWith(Backend.INDEXSTORE_NAME)) {
            cfm.caching(CachingOptions.ROWS_ONLY);
        }

        // Configure sstable compression
        final CompressionParameters cp;
        if (compressionEnabled) {
            try {
                cp = new CompressionParameters(compressionClass,
                        compressionChunkSizeKB * 1024,
                        Collections.<String, String>emptyMap());
                // CompressionParameters doesn't override toString(), so be explicit
                log.debug("Creating CF {}: setting {}={} and {}={} on {}",
                        new Object[]{
                                columnfamilyName,
                                CompressionParameters.SSTABLE_COMPRESSION, compressionClass,
                                CompressionParameters.CHUNK_LENGTH_KB, compressionChunkSizeKB,
                                cp});
            } catch (ConfigurationException ce) {
                throw new PermanentBackendException(ce);
            }
        } else {
            cp = new CompressionParameters(null);
            log.debug("Creating CF {}: setting {} to null to disable compression",
                    columnfamilyName, CompressionParameters.SSTABLE_COMPRESSION);
        }
        cfm.compressionParameters(cp);

        try {
            cfm.addDefaultIndexNames();
        } catch (ConfigurationException e) {
            throw new PermanentBackendException("Failed to create column family metadata for " + keyspaceName + ":" + columnfamilyName, e);
        }
        try {
            MigrationManager.announceNewColumnFamily(cfm);
            log.info("Created CF {} in KS {}", columnfamilyName, keyspaceName);
        } catch (ConfigurationException e) {
            throw new PermanentBackendException("Failed to create column family " + keyspaceName + ":" + columnfamilyName, e);
        }

        /*
         * I'm chasing a nondetermistic exception that appears only rarely on my
         * machine when executing the embedded cassandra tests. If these dummy
         * reads ever actually fail and dump a log message, it could help debug
         * the root cause.
         *
         *   java.lang.RuntimeException: java.lang.IllegalArgumentException: Unknown table/cf pair (InternalCassandraEmbeddedKeyColumnValueTest.testStore1)
         *          at org.apache.cassandra.service.StorageProxy$DroppableRunnable.run(StorageProxy.java:1582)
         *           at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1145)
         *           at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:615)
         *           at java.lang.Thread.run(Thread.java:744)
         *   Caused by: java.lang.IllegalArgumentException: Unknown table/cf pair (InternalCassandraEmbeddedKeyColumnValueTest.testStore1)
         *           at org.apache.cassandra.db.Table.getColumnFamilyStore(Table.java:166)
         *           at org.apache.cassandra.db.Table.getRow(Table.java:354)
         *           at org.apache.cassandra.db.SliceFromReadCommand.getRow(SliceFromReadCommand.java:70)
         *           at org.apache.cassandra.service.StorageProxy$LocalReadRunnable.runMayThrow(StorageProxy.java:1052)
         *           at org.apache.cassandra.service.StorageProxy$DroppableRunnable.run(StorageProxy.java:1578)
         *           ... 3 more
         */
        retryDummyRead(keyspaceName, columnfamilyName);
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws BackendException {

        CFMetaData cfm = Schema.instance.getCFMetaData(keySpaceName, cf);

        if (cfm == null)
            return null;

        return ImmutableMap.copyOf(cfm.compressionParameters().asThriftOptions());
    }

    private void retryDummyRead(String ks, String cf) throws PermanentBackendException {

        final long limit = System.currentTimeMillis() + (60L * 1000L);

        while (System.currentTimeMillis() < limit) {
            try {
                SortedSet<CellName> names = new TreeSet<>(new Comparator<CellName>() {
                    // This is a singleton set.  We need to define a comparator because SimpleDenseCellName is not
                    // comparable, but it doesn't have to be a useful comparator
                    @Override
                    public int compare(CellName o1, CellName o2)
                    {
                        return 0;
                    }
                });
                names.add(CellNames.simpleDense(ByteBufferUtil.zeroByteBuffer(1)));
                NamesQueryFilter nqf = new NamesQueryFilter(names);
                SliceByNamesReadCommand cmd = new SliceByNamesReadCommand(ks, ByteBufferUtil.zeroByteBuffer(1), cf, 1L, nqf);
                StorageProxy.read(ImmutableList.<ReadCommand> of(cmd), ConsistencyLevel.QUORUM);
                log.info("Read on CF {} in KS {} succeeded", cf, ks);
                return;
            } catch (Throwable t) {
                log.warn("Failed to read CF {} in KS {} following creation", cf, ks, t);
            }

            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new PermanentBackendException(e);
            }
        }

        throw new PermanentBackendException("Timed out while attempting to read CF " + cf + " in KS " + ks + " following creation");
    }
}

package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeoutException;

import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraHelper;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.CFMetaData.Caching;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.SliceByNamesReadCommand;
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
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.utils.CassandraDaemonWrapper;

public class CassandraEmbeddedStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(CassandraEmbeddedStoreManager.class);

    /**
     * The default value for
     * {@link GraphDatabaseConfiguration#STORAGE_CONF_FILE}.
     */
    public static final String CASSANDRA_YAML_DEFAULT = "./conf/cassandra.yaml";

    private final Map<String, CassandraEmbeddedKeyColumnValueStore> openStores;

    private final IRequestScheduler requestScheduler;

    public CassandraEmbeddedStoreManager(Configuration config) throws StorageException {
        super(config);

        // Check if we have non-default thrift frame size or max message size set and warn users
        // because embedded doesn't use Thrift, warning is good enough here otherwise it would
        // make bad user experience if we don't warn at all or crash on this.
        if (config.has(CASSANDRA_THRIFT_FRAME_SIZE))
            log.warn("Couldn't set custom Thrift Frame Size property, use 'cassandrathrift' instead.");

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
    @SuppressWarnings("unchecked")
    public IPartitioner<? extends Token<?>> getCassandraPartitioner()
            throws StorageException {
        try {
            return StorageService.getPartitioner();
        } catch (Exception e) {
            log.warn("Could not read local token range: {}", e);
            throw new PermanentStorageException("Could not read partitioner information on cluster", e);
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
    public synchronized KeyColumnValueStore openDatabase(String name) throws StorageException {
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
    public List<KeyRange> getLocalKeyPartition() throws StorageException {
        @SuppressWarnings("rawtypes")
        Collection<Range<Token>> ranges = StorageService.instance.getLocalPrimaryRanges(keySpaceName);
        List<KeyRange> keyRanges = new ArrayList<KeyRange>(ranges.size());

        for (@SuppressWarnings("rawtypes") Range<Token> range : ranges) {
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
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        Preconditions.checkNotNull(mutations);

        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        int size = 0;
        for (Map<StaticBuffer, KCVMutation> mutation : mutations.values()) size += mutation.size();
        Map<StaticBuffer, RowMutation> rowMutations = new HashMap<StaticBuffer, RowMutation>(size);

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> mutEntry : mutations.entrySet()) {
            String columnFamily = mutEntry.getKey();
            for (Map.Entry<StaticBuffer, KCVMutation> titanMutation : mutEntry.getValue().entrySet()) {
                StaticBuffer key = titanMutation.getKey();
                KCVMutation mut = titanMutation.getValue();

                RowMutation rm = rowMutations.get(key);
                if (rm == null) {
                    rm = new RowMutation(keySpaceName, key.asByteBuffer());
                    rowMutations.put(key, rm);
                }

                if (mut.hasAdditions()) {
                    for (Entry e : mut.getAdditions()) {
                        rm.add(columnFamily, e.getColumnAs(StaticBuffer.BB_FACTORY), e.getValueAs(StaticBuffer.BB_FACTORY), commitTime.getAdditionTime(times.getUnit()));

                    }
                }

                if (mut.hasDeletions()) {
                    for (StaticBuffer col : mut.getDeletions()) {
                        rm.delete(columnFamily, col.as(StaticBuffer.BB_FACTORY), commitTime.getDeletionTime(times.getUnit()));
                    }
                }

            }
        }

        mutate(new ArrayList<RowMutation>(rowMutations.values()), getTx(txh).getWriteConsistencyLevel().getDB());

        sleepAfterWrite(txh, commitTime);
    }

    private void mutate(List<RowMutation> cmds, org.apache.cassandra.db.ConsistencyLevel clvl) throws StorageException {
        try {
            schedule(DatabaseDescriptor.getRpcTimeout());
            try {
                StorageProxy.mutate(cmds, clvl);
            } catch (RequestExecutionException e) {
                throw new TemporaryStorageException(e);
            } finally {
                release();
            }
        } catch (TimeoutException ex) {
            log.debug("Cassandra TimeoutException", ex);
            throw new TemporaryStorageException(ex);
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
    public void clearStorage() throws StorageException {
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
            throw new PermanentStorageException(e);
        }
    }

    private void ensureKeyspaceExists(String keyspaceName) throws StorageException {

        if (null != Schema.instance.getKeyspaceInstance(keyspaceName))
            return;

        // Keyspace not found; create it
        String strategyName = storageConfig.get(REPLICATION_STRATEGY);

        KSMetaData ksm;
        try {
            ksm = KSMetaData.newKeyspace(keyspaceName, strategyName, strategyOptions, true);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to instantiate keyspace metadata for " + keyspaceName, e);
        }
        try {
            MigrationManager.announceNewKeyspace(ksm);
            log.info("Created keyspace {}", keyspaceName);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to create keyspace " + keyspaceName, e);
        }
    }

    private void ensureColumnFamilyExists(String ksName, String cfName) throws StorageException {
        ensureColumnFamilyExists(ksName, cfName, BytesType.instance);
    }

    private void ensureColumnFamilyExists(String keyspaceName, String columnfamilyName, AbstractType<?> comparator) throws StorageException {
        if (null != Schema.instance.getCFMetaData(keyspaceName, columnfamilyName))
            return;

        // Column Family not found; create it
        CFMetaData cfm = new CFMetaData(keyspaceName, columnfamilyName, ColumnFamilyType.Standard, comparator, null);

        // Hard-coded caching settings
        if (columnfamilyName.startsWith(Backend.EDGESTORE_NAME)) {
            cfm.caching(Caching.KEYS_ONLY);
        } else if (columnfamilyName.startsWith(Backend.INDEXSTORE_NAME)) {
            cfm.caching(Caching.ROWS_ONLY);
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
                throw new PermanentStorageException(ce);
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
            throw new PermanentStorageException("Failed to create column family metadata for " + keyspaceName + ":" + columnfamilyName, e);
        }
        try {
            MigrationManager.announceNewColumnFamily(cfm);
            log.info("Created CF {} in KS {}", columnfamilyName, keyspaceName);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to create column family " + keyspaceName + ":" + columnfamilyName, e);
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
    public Map<String, String> getCompressionOptions(String cf) throws StorageException {

        CFMetaData cfm = Schema.instance.getCFMetaData(keySpaceName, cf);

        if (cfm == null)
            return null;

        return ImmutableMap.copyOf(cfm.compressionParameters().asThriftOptions());
    }

    private void retryDummyRead(String ks, String cf) throws PermanentStorageException {

        final long limit = System.currentTimeMillis() + (60L * 1000L);

        while (System.currentTimeMillis() < limit) {
            try {
                SortedSet<ByteBuffer> ss = new TreeSet<ByteBuffer>();
                ss.add(ByteBufferUtil.zeroByteBuffer(1));
                NamesQueryFilter nqf = new NamesQueryFilter(ss);
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
                throw new PermanentStorageException(e);
            }
        }

        throw new PermanentStorageException("Timed out while attempting to read CF " + cf + " in KS " + ks + " following creation");
    }
}

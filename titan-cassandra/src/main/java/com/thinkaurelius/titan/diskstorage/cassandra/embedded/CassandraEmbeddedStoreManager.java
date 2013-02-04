package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.CFMetaData.Caching;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.dht.BytesToken;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.scheduler.IRequestScheduler;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;

public class CassandraEmbeddedStoreManager extends AbstractCassandraStoreManager {

    private static final Logger log = LoggerFactory.getLogger(CassandraEmbeddedStoreManager.class);

    /**
     * When non-empty, the CassandraEmbeddedStoreManager constructor will copy
     * the value to the "cassandra.config" system property and start a
     * backgrounded cassandra daemon thread. cassandra's static initializers
     * will interpret the "cassandra.config" system property as a url pointing
     * to a cassandra.yaml file.
     * <p/>
     * An example value of this variable is
     * "file:///home/dalaro/titan/target/cassandra-tmp/conf/127.0.0.1/cassandra.yaml".
     * <p/>
     * When empty, the constructor does none of the steps described above.
     * <p/>
     * The constructor logic described above is also internally synchronized in
     * order to start Cassandra at most once in a thread-safe manner. Subsequent
     * constructor invocations (or concurrent invocations which enter the
     * internal synchronization block after the first) with a nonempty value for
     * this variable will behave as though an empty value was set.
     * <p/>
     * Value = {@value}
     */
    public static final String CASSANDRA_CONFIG_DIR_DEFAULT = "./config/cassandra.yaml";
    public static final String CASSANDRA_CONFIG_DIR_KEY = "cassandra-config-dir";

    private final Map<String, CassandraEmbeddedKeyColumnValueStore> openStores;

    private final IRequestScheduler requestScheduler;

    public CassandraEmbeddedStoreManager(Configuration config) throws StorageException {
        super(config);
        String cassandraConfigDir = config.getString(CASSANDRA_CONFIG_DIR_KEY, CASSANDRA_CONFIG_DIR_DEFAULT);

        assert cassandraConfigDir != null && !cassandraConfigDir.isEmpty();

        CassandraDaemonWrapper.start(cassandraConfigDir);

        this.openStores = new HashMap<String, CassandraEmbeddedKeyColumnValueStore>(8);
        this.requestScheduler = DatabaseDescriptor.getRequestScheduler();
    }

    @Override
    public Partitioner getPartitioner() throws StorageException {
        try {
            return Partitioner.getPartitioner(StorageService.getPartitioner());
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
    }

    @Override
    public synchronized KeyColumnValueStore openDatabase(String name)
            throws StorageException {
        if (openStores.containsKey(name)) return openStores.get(name);
        else {
            // Ensure that both the keyspace and column family exist
            ensureKeyspaceExists(keySpaceName);
            ensureColumnFamilyExists(keySpaceName, name);

            CassandraEmbeddedKeyColumnValueStore store = new CassandraEmbeddedKeyColumnValueStore(keySpaceName,
                    name, this);
            openStores.put(name, store);
            return store;
        }
    }

    ByteBuffer[] getLocalKeyPartition() throws StorageException {
        // getLocalPrimaryRange() returns a raw type
        @SuppressWarnings("rawtypes")
        Range<Token> range = StorageService.instance.getLocalPrimaryRange();
        Token<?> leftKeyExclusive = range.left;
        Token<?> rightKeyInclusive = range.right;

        if (leftKeyExclusive instanceof BytesToken) {
            assert rightKeyInclusive instanceof BytesToken;

            // l is exclusive, r is inclusive
            BytesToken l = (BytesToken) leftKeyExclusive;
            BytesToken r = (BytesToken) rightKeyInclusive;

            Preconditions.checkArgument(l.token.length == r.token.length, "Tokens have unequal length");
            int tokenLength = l.token.length;
            log.debug("Token length: " + tokenLength);

            byte[][] tokens = new byte[][]{l.token, r.token};
            byte[][] plusOne = new byte[2][tokenLength];

            for (int j = 0; j < 2; j++) {
                boolean carry = true;
                for (int i = tokenLength - 1; i >= 0; i--) {
                    byte b = tokens[j][i];
                    if (carry) {
                        b++;
                        carry = false;
                    }
                    if (b == 0) carry = true;
                    plusOne[j][i] = b;
                }
            }

            ByteBuffer lb = ByteBuffer.wrap(plusOne[0]);
            ByteBuffer rb = ByteBuffer.wrap(plusOne[1]);
            Preconditions.checkArgument(lb.remaining() == tokenLength, lb.remaining());
            Preconditions.checkArgument(rb.remaining() == tokenLength, rb.remaining());

            return new ByteBuffer[]{lb, rb};
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /*
      * This implementation can't handle counter columns.
      *
      * The private method internal_batch_mutate in CassandraServer as of 1.2.0
      * provided most of the following method after transaction handling.
      */
    @Override
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> mutations, StoreTransaction txh) throws StorageException {
        Preconditions.checkNotNull(mutations);

        long deletionTimestamp = TimeUtility.getApproxNSSinceEpoch(false);
        long additionTimestamp = TimeUtility.getApproxNSSinceEpoch(true);

        int size = 0;
        for (Map<ByteBuffer, Mutation> mutation : mutations.values()) size += mutation.size();
        Map<ByteBuffer, RowMutation> rowMutations = new HashMap<ByteBuffer, RowMutation>(size);

        for (Map.Entry<String, Map<ByteBuffer, Mutation>> mutEntry : mutations.entrySet()) {
            String columnFamily = mutEntry.getKey();
            for (Map.Entry<ByteBuffer, Mutation> titanMutation : mutEntry.getValue().entrySet()) {
                ByteBuffer key = titanMutation.getKey().duplicate();
                Mutation mut = titanMutation.getValue();

                RowMutation rm = rowMutations.get(key);
                if (rm == null) {
                    rm = new RowMutation(keySpaceName, key);
                    rowMutations.put(key, rm);
                }

                if (mut.hasAdditions()) {
                    for (Entry e : mut.getAdditions()) {
                        QueryPath path = new QueryPath(columnFamily, null, e.getColumn().duplicate());
                        rm.add(path, e.getValue().duplicate(), additionTimestamp);
                    }
                }

                if (mut.hasDeletions()) {
                    for (ByteBuffer col : mut.getDeletions()) {
                        QueryPath path = new QueryPath(columnFamily, null, col.duplicate());
                        rm.delete(path, deletionTimestamp);
                    }
                }

            }
        }

        mutate(new ArrayList<RowMutation>(rowMutations.values()), getTx(txh).getWriteConsistencyLevel().getDBConsistency());
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

        if (null != Schema.instance.getTableInstance(keyspaceName))
            return;

        // Keyspace not found; create it
        String strategyName = "org.apache.cassandra.locator.SimpleStrategy";
        Map<String, String> options = new HashMap<String, String>() {{
            put("replication_factor", String.valueOf(replicationFactor));
        }};

        KSMetaData ksm;
        try {
            ksm = KSMetaData.newKeyspace(keyspaceName, strategyName, options, true);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to instantiate keyspace metadata for " + keyspaceName, e);
        }
        try {
            MigrationManager.announceNewKeyspace(ksm);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to create keyspace " + keyspaceName, e);
        }
    }

    private void ensureColumnFamilyExists(String keyspaceName, String columnfamilyName) throws StorageException {
        if (null != Schema.instance.getCFMetaData(keyspaceName, columnfamilyName))
            return;

        // Column Family not found; create it
        CFMetaData cfm = new CFMetaData(keyspaceName, columnfamilyName, ColumnFamilyType.Standard, BytesType.instance, null);

        // Hard-coded caching settings
        if (columnfamilyName.startsWith(Backend.EDGESTORE_NAME)) {
            cfm.caching(Caching.KEYS_ONLY);
        } else if (columnfamilyName.startsWith(Backend.VERTEXINDEX_STORE_NAME)) {
            cfm.caching(Caching.ROWS_ONLY);
        }

        try {
            cfm.addDefaultIndexNames();
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to create column family metadata for " + keyspaceName + ":" + columnfamilyName, e);
        }
        try {
            MigrationManager.announceNewColumnFamily(cfm);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to create column family " + keyspaceName + ":" + columnfamilyName, e);
        }
    }

    @Override
    public String getConfigurationProperty(final String key) {
        KSMetaData ksMetaData = Schema.instance.getKSMetaData(keySpaceName);
        Preconditions.checkNotNull(ksMetaData);
        return ksMetaData.strategyOptions.get(key);
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        KSMetaData current = Schema.instance.getKSMetaData(keySpaceName);
        Preconditions.checkNotNull(current);

        KSMetaData ksUpdate = KSMetaData.cloneWith(current, Collections.<CFMetaData>emptyList());
        ksUpdate.strategyOptions.put(key, value);

        try {
            MigrationManager.announceKeyspaceUpdate(ksUpdate);
        } catch (ConfigurationException e) {
            throw new PermanentStorageException("Failed to update config property", e);
        }
    }
}

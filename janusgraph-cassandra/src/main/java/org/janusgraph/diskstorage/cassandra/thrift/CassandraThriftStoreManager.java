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

package org.janusgraph.diskstorage.cassandra.thrift;

import static org.janusgraph.diskstorage.cassandra.CassandraTransaction.getTx;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.*;
import org.janusgraph.diskstorage.cassandra.utils.CassandraHelper;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.util.system.NetworkUtil;

import org.apache.cassandra.dht.AbstractByteOrderedPartitioner;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager;
import org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory;
import org.janusgraph.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import static org.janusgraph.diskstorage.configuration.ConfigOption.disallowEmpty;

/**
 * This class creates {@see CassandraThriftKeyColumnValueStore}s and
 * handles Cassandra-backed allocation of vertex IDs for JanusGraph (when so
 * configured).
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
@PreInitializeConfigOptions
public class CassandraThriftStoreManager extends AbstractCassandraStoreManager {

    public enum PoolExhaustedAction {
        BLOCK(GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK),
        FAIL(GenericKeyedObjectPool.WHEN_EXHAUSTED_FAIL),
        GROW(GenericKeyedObjectPool.WHEN_EXHAUSTED_GROW);

        private final byte b;

        PoolExhaustedAction(byte b) {
            this.b = b;
        }

        public byte getByte() {
            return b;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(CassandraThriftStoreManager.class);

    public static final ConfigNamespace THRIFT_NS =
            new ConfigNamespace(AbstractCassandraStoreManager.CASSANDRA_NS, "thrift",
                    "Options for JanusGraph's own Thrift Cassandra backend");

    public static final ConfigNamespace CPOOL_NS =
            new ConfigNamespace(THRIFT_NS, "cpool", "Options for the Apache commons-pool connection manager");

    public static final ConfigOption<String> CPOOL_WHEN_EXHAUSTED =
            new ConfigOption<>(CPOOL_NS, "when-exhausted",
            "What to do when clients concurrently request more active connections than are allowed " +
            "by the pool.  The value must be one of BLOCK, FAIL, or GROW.",
            ConfigOption.Type.MASKABLE, String.class, PoolExhaustedAction.BLOCK.toString(),
            disallowEmpty(String.class));

    public static final ConfigOption<Integer> CPOOL_MAX_TOTAL =
            new ConfigOption<Integer>(CPOOL_NS, "max-total",
            "Max number of allowed Thrift connections, idle or active (-1 to leave undefined)",
            ConfigOption.Type.MASKABLE, -1);

    public static final ConfigOption<Integer> CPOOL_MAX_ACTIVE =
            new ConfigOption<Integer>(CPOOL_NS, "max-active",
            "Maximum number of concurrently in-use connections (-1 to leave undefined)",
            ConfigOption.Type.MASKABLE, 16);

    public static final ConfigOption<Integer> CPOOL_MAX_IDLE =
            new ConfigOption<Integer>(CPOOL_NS, "max-idle",
            "Maximum number of concurrently idle connections (-1 to leave undefined)",
            ConfigOption.Type.MASKABLE, 4);

    public static final ConfigOption<Integer> CPOOL_MIN_IDLE =
            new ConfigOption<Integer>(CPOOL_NS, "min-idle",
            "Minimum number of idle connections the pool attempts to maintain",
            ConfigOption.Type.MASKABLE, 0);

    // Wart: allowing -1 like commons-pool's convention precludes using StandardDuration
    public static final ConfigOption<Long> CPOOL_MAX_WAIT =
            new ConfigOption<Long>(CPOOL_NS, "max-wait",
            "Maximum number of milliseconds to block when " + ConfigElement.getPath(CPOOL_WHEN_EXHAUSTED) +
            " is set to BLOCK.  Has no effect when set to actions besides BLOCK.  Set to -1 to wait indefinitely.",
            ConfigOption.Type.MASKABLE, -1L);

    // Wart: allowing -1 like commons-pool's convention precludes using StandardDuration
    public static final ConfigOption<Long> CPOOL_EVICTOR_PERIOD =
            new ConfigOption<Long>(CPOOL_NS, "evictor-period",
            "Approximate number of milliseconds between runs of the idle connection evictor.  " +
            "Set to -1 to never run the idle connection evictor.",
            ConfigOption.Type.MASKABLE, 30L * 1000L);

    // Wart: allowing -1 like commons-pool's convention precludes using StandardDuration
    public static final ConfigOption<Long> CPOOL_MIN_EVICTABLE_IDLE_TIME =
            new ConfigOption<Long>(CPOOL_NS, "min-evictable-idle-time",
            "Minimum number of milliseconds a connection must be idle before it is eligible for " +
            "eviction.  See also " + ConfigElement.getPath(CPOOL_EVICTOR_PERIOD) + ".  Set to -1 to never evict " +
            "idle connections.", ConfigOption.Type.MASKABLE, 60L * 1000L);

    public static final ConfigOption<Boolean> CPOOL_IDLE_TESTS =
            new ConfigOption<Boolean>(CPOOL_NS, "idle-test",
            "Whether the idle connection evictor validates idle connections and drops those that fail to validate",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<Integer> CPOOL_IDLE_TESTS_PER_EVICTION_RUN =
            new ConfigOption<Integer>(CPOOL_NS, "idle-tests-per-eviction-run",
            "When the value is negative, e.g. -n, roughly one nth of the idle connections are tested per run.  " +
            "When the value is positive, e.g. n, the min(idle-count, n) connections are tested per run.",
            ConfigOption.Type.MASKABLE, 0);


    private final Map<String, CassandraThriftKeyColumnValueStore> openStores;
    private final CTConnectionPool pool;
    private final Deployment deployment;

    public CassandraThriftStoreManager(Configuration config) throws BackendException {
        super(config);

        /*
         * This is eventually passed to Thrift's TSocket constructor. The
         * constructor parameter is of type int.
         */
        int thriftTimeoutMS = (int)config.get(GraphDatabaseConfiguration.CONNECTION_TIMEOUT).toMillis();

        CTConnectionFactory.Config factoryConfig = new CTConnectionFactory.Config(hostnames, port, username, password)
                                                                            .setTimeoutMS(thriftTimeoutMS)
                                                                            .setFrameSize(thriftFrameSizeBytes);

        if (config.get(SSL_ENABLED)) {
            factoryConfig.setSSLTruststoreLocation(config.get(SSL_TRUSTSTORE_LOCATION));
            factoryConfig.setSSLTruststorePassword(config.get(SSL_TRUSTSTORE_PASSWORD));
        }

        final PoolExhaustedAction poolExhaustedAction = ConfigOption.getEnumValue(
                config.get(CPOOL_WHEN_EXHAUSTED), PoolExhaustedAction.class);

        CTConnectionPool p = new CTConnectionPool(factoryConfig.build());
        p.setTestOnBorrow(true);
        p.setTestOnReturn(true);
        p.setTestWhileIdle(config.get(CPOOL_IDLE_TESTS));
        p.setNumTestsPerEvictionRun(config.get(CPOOL_IDLE_TESTS_PER_EVICTION_RUN));
        p.setWhenExhaustedAction(poolExhaustedAction.getByte());
        p.setMaxActive(config.get(CPOOL_MAX_ACTIVE));
        p.setMaxTotal(config.get(CPOOL_MAX_TOTAL)); // maxTotal limits active + idle
        p.setMaxIdle(config.get(CPOOL_MAX_IDLE));
        p.setMinIdle(config.get(CPOOL_MIN_IDLE));
        p.setMaxWait(config.get(CPOOL_MAX_WAIT));
        p.setTimeBetweenEvictionRunsMillis(config.get(CPOOL_EVICTOR_PERIOD));
        p.setMinEvictableIdleTimeMillis(config.get(CPOOL_MIN_EVICTABLE_IDLE_TIME));
        this.pool = p;

        this.openStores = new HashMap<String, CassandraThriftKeyColumnValueStore>();

        // Only watch the ring and change endpoints with BOP
        if (getCassandraPartitioner() instanceof ByteOrderedPartitioner) {
            deployment = (hostnames.length == 1)// mark deployment as local only in case we have byte ordered partitioner and local connection
                          ? (NetworkUtil.isLocalConnection(hostnames[0])) ? Deployment.LOCAL : Deployment.REMOTE
                          : Deployment.REMOTE;
        } else {
            deployment = Deployment.REMOTE;
        }
    }

    @Override
    public Deployment getDeployment() {
        return deployment;
    }

    @Override
    public IPartitioner getCassandraPartitioner() throws BackendException {
        CTConnection conn = null;
        try {
            conn = pool.borrowObject(SYSTEM_KS);
            return FBUtilities.newPartitioner(conn.getClient().describe_partitioner());
        } catch (Exception e) {
            throw new TemporaryBackendException(e);
        } finally {
            pool.returnObjectUnsafe(SYSTEM_KS, conn);
        }
    }

    @Override
    public String toString() {
        return "thriftCassandra" + super.toString();
    }

    @Override
    public void close() throws BackendException {
        openStores.clear();
        closePool();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        Preconditions.checkNotNull(mutations);

        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);

        ConsistencyLevel consistency = getTx(txh).getWriteConsistencyLevel().getThrift();

        // Generate Thrift-compatible batch_mutate() datastructure
        // key -> cf -> cassmutation
        int size = 0;
        for (Map<StaticBuffer, KCVMutation> mutation : mutations.values()) size += mutation.size();
        Map<ByteBuffer, Map<String, List<org.apache.cassandra.thrift.Mutation>>> batch =
                new HashMap<ByteBuffer, Map<String, List<org.apache.cassandra.thrift.Mutation>>>(size);


        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> keyMutation : mutations.entrySet()) {
            String columnFamily = keyMutation.getKey();
            for (Map.Entry<StaticBuffer, KCVMutation> mutEntry : keyMutation.getValue().entrySet()) {
                ByteBuffer keyBB = mutEntry.getKey().asByteBuffer();

                // Get or create the single Cassandra Mutation object responsible for this key
                Map<String, List<org.apache.cassandra.thrift.Mutation>> cfmutation = batch.get(keyBB);
                if (cfmutation == null) {
                    cfmutation = new HashMap<String, List<org.apache.cassandra.thrift.Mutation>>(3); // Most mutations only modify the edgeStore and indexStore
                    batch.put(keyBB, cfmutation);
                }

                KCVMutation mutation = mutEntry.getValue();
                List<org.apache.cassandra.thrift.Mutation> thriftMutation =
                        new ArrayList<org.apache.cassandra.thrift.Mutation>(mutations.size());

                if (mutation.hasDeletions()) {
                    for (StaticBuffer buf : mutation.getDeletions()) {
                        Deletion d = new Deletion();
                        SlicePredicate sp = new SlicePredicate();
                        sp.addToColumn_names(buf.as(StaticBuffer.BB_FACTORY));
                        d.setPredicate(sp);
                        d.setTimestamp(commitTime.getDeletionTime(times));
                        org.apache.cassandra.thrift.Mutation m = new org.apache.cassandra.thrift.Mutation();
                        m.setDeletion(d);
                        thriftMutation.add(m);
                    }
                }

                if (mutation.hasAdditions()) {
                    for (Entry ent : mutation.getAdditions()) {
                        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
                        Column column = new Column(ent.getColumnAs(StaticBuffer.BB_FACTORY));
                        column.setValue(ent.getValueAs(StaticBuffer.BB_FACTORY));

                        column.setTimestamp(commitTime.getAdditionTime(times));

                        Integer ttl = (Integer) ent.getMetaData().get(EntryMetaData.TTL);
                        if (null != ttl && ttl > 0) {
                            column.setTtl(ttl);
                        }

                        cosc.setColumn(column);
                        org.apache.cassandra.thrift.Mutation m = new org.apache.cassandra.thrift.Mutation();
                        m.setColumn_or_supercolumn(cosc);
                        thriftMutation.add(m);
                    }
                }

                cfmutation.put(columnFamily, thriftMutation);
            }
        }

        CTConnection conn = null;
        try {
            conn = pool.borrowObject(keySpaceName);
            Cassandra.Client client = conn.getClient();
            if (atomicBatch) {
                client.atomic_batch_mutate(batch, consistency);
            } else {
                client.batch_mutate(batch, consistency);
            }
        } catch (Exception ex) {
            throw CassandraThriftKeyColumnValueStore.convertException(ex);
        } finally {
            pool.returnObjectUnsafe(keySpaceName, conn);
        }

        sleepAfterWrite(txh, commitTime);
    }

    @Override // TODO: *BIG FAT WARNING* 'synchronized is always *bad*, change openStores to use ConcurrentLinkedHashMap
    public synchronized CassandraThriftKeyColumnValueStore openDatabase(final String name, StoreMetaData.Container metaData) throws BackendException {
        if (openStores.containsKey(name))
            return openStores.get(name);

        ensureColumnFamilyExists(keySpaceName, name);

        CassandraThriftKeyColumnValueStore store = new CassandraThriftKeyColumnValueStore(keySpaceName, name, this, pool);
        openStores.put(name, store);
        return store;
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        CTConnection conn = null;
        IPartitioner partitioner = getCassandraPartitioner();

        if (!(partitioner instanceof AbstractByteOrderedPartitioner))
            throw new UnsupportedOperationException("getLocalKeyPartition() only supported by byte ordered partitioner.");

        Token.TokenFactory tokenFactory = partitioner.getTokenFactory();

        try {
            // Resist the temptation to describe SYSTEM_KS.  It has no ring.
            // Instead, we'll create our own keyspace (or check that it exists), then describe it.
            ensureKeyspaceExists(keySpaceName);

            conn = pool.borrowObject(keySpaceName);
            List<TokenRange> ranges  = conn.getClient().describe_ring(keySpaceName);
            List<KeyRange> keyRanges = new ArrayList<KeyRange>(ranges.size());

            for (TokenRange range : ranges) {
                if (!NetworkUtil.hasLocalAddress(range.endpoints))
                    continue;

                keyRanges.add(CassandraHelper.transformRange(tokenFactory.fromString(range.start_token), tokenFactory.fromString(range.end_token)));
            }

            return keyRanges;
        } catch (Exception e) {
            throw CassandraThriftKeyColumnValueStore.convertException(e);
        } finally {
            pool.returnObjectUnsafe(keySpaceName, conn);
        }
    }

    /**
     * Connect to Cassandra via Thrift on the specified host and port and attempt to truncate the named keyspace.
     * <p/>
     * This is a utility method intended mainly for testing. It is
     * equivalent to issuing 'truncate <cf>' for each of the column families in keyspace using
     * the cassandra-cli tool.
     * <p/>
     * Using truncate is better for a number of reasons, most significantly because it doesn't
     * involve any schema modifications which can take time to propagate across the cluster such
     * leaves nodes in the inconsistent state and could result in read/write failures.
     * Any schema modifications are discouraged until there is no traffic to Keyspace or ColumnFamilies.
     *
     * @throws org.janusgraph.diskstorage.BackendException if any checked Thrift or UnknownHostException is thrown in the body of this method
     */
    public void clearStorage() throws BackendException {
        openStores.clear();
        final String lp = "ClearStorage: "; // "log prefix"
        /*
         * log4j is capable of automatically writing the name of a method that
         * generated a log message, but the docs warn that "generating caller
         * location information is extremely slow and should be avoided unless
         * execution speed is not an issue."
         */

        CTConnection conn = null;
        try {
            conn = pool.borrowObject(SYSTEM_KS);
            Cassandra.Client client = conn.getClient();

            KsDef ksDef;
            try {
                client.set_keyspace(keySpaceName);
                ksDef = client.describe_keyspace(keySpaceName);
            } catch (NotFoundException e) {
                log.debug(lp + "Keyspace {} does not exist, not attempting to truncate.", keySpaceName);
                return;
            } catch (InvalidRequestException e) {
                log.debug(lp + "InvalidRequestException when attempting to describe keyspace {}, not attempting to truncate.", keySpaceName);
                return;
            }


            if (null == ksDef) {
                log.debug(lp + "Received null KsDef for keyspace {}; not truncating its CFs", keySpaceName);
                return;
            }

            List<CfDef> cfDefs = ksDef.getCf_defs();

            if (null == cfDefs) {
                log.debug(lp + "Received empty CfDef list for keyspace {}; not truncating CFs", keySpaceName);
                return;
            }

            for (CfDef cfDef : ksDef.getCf_defs()) {
                client.truncate(cfDef.name);
                log.info(lp + "Truncated CF {} in keyspace {}", cfDef.name, keySpaceName);
            }

            /*
             * Clearing the CTConnectionPool is unnecessary. This method
             * removes no keyspaces. All open Cassandra connections will
             * remain valid.
             */
        } catch (Exception e) {
            throw new TemporaryBackendException(e);
        } finally {
            if (conn != null && conn.getClient() != null) {
                try {
                    conn.getClient().set_keyspace(SYSTEM_KS);
                } catch (InvalidRequestException e) {
                    log.warn("Failed to reset keyspace", e);
                } catch (TException e) {
                    log.warn("Failed to reset keyspace", e);
                }
            }
            pool.returnObjectUnsafe(SYSTEM_KS, conn);
        }
    }

    private KsDef ensureKeyspaceExists(String keyspaceName) throws TException, BackendException {
        CTConnection connection = null;

        try {
            connection = pool.borrowObject(SYSTEM_KS);
            Cassandra.Client client = connection.getClient();

            try {
                // Side effect: throws Exception if keyspaceName doesn't exist
                client.set_keyspace(keyspaceName); // Don't remove
                client.set_keyspace(SYSTEM_KS);
                log.debug("Found existing keyspace {}", keyspaceName);
            } catch (InvalidRequestException e) {
                // Keyspace didn't exist; create it
                log.debug("Creating keyspace {}...", keyspaceName);

                KsDef ksdef = new KsDef().setName(keyspaceName)
                        .setCf_defs(new LinkedList<CfDef>()) // cannot be null but can be empty
                        .setStrategy_class(storageConfig.get(REPLICATION_STRATEGY))
                        .setStrategy_options(strategyOptions);

                client.set_keyspace(SYSTEM_KS);
                try {
                    client.system_add_keyspace(ksdef);
                    retrySetKeyspace(keyspaceName, client);
                    log.debug("Created keyspace {}", keyspaceName);
                } catch (InvalidRequestException ire) {
                    log.error("system_add_keyspace failed for keyspace=" + keyspaceName, ire);
                    throw ire;
                }

            }

            return client.describe_keyspace(keyspaceName);
        } catch (Exception e) {
            throw new TemporaryBackendException(e);
        } finally {
            pool.returnObjectUnsafe(SYSTEM_KS, connection);
        }
    }

    private void retrySetKeyspace(String ksName, Cassandra.Client client) throws BackendException {
        final long end = System.currentTimeMillis() + (60L * 1000L);

        while (System.currentTimeMillis() <= end) {
            try {
                client.set_keyspace(ksName);
                return;
            } catch (Exception e) {
                log.warn("Exception when changing to keyspace {} after creating it", ksName, e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                    throw new PermanentBackendException("Unexpected interrupt (shutting down?)", ie);
                }
            }
        }

        throw new PermanentBackendException("Could change to keyspace " + ksName + " after creating it");
    }

    private void ensureColumnFamilyExists(String ksName, String cfName) throws BackendException {
        ensureColumnFamilyExists(ksName, cfName, "org.apache.cassandra.db.marshal.BytesType");
    }

    private void ensureColumnFamilyExists(String ksName, String cfName, String comparator) throws BackendException {
        CTConnection conn = null;
        try {
            KsDef keyspaceDef = ensureKeyspaceExists(ksName);

            conn = pool.borrowObject(ksName);
            Cassandra.Client client = conn.getClient();

            log.debug("Looking up metadata on keyspace {}...", ksName);

            boolean foundColumnFamily = false;
            for (CfDef cfDef : keyspaceDef.getCf_defs()) {
                String curCfName = cfDef.getName();
                if (curCfName.equals(cfName))
                    foundColumnFamily = true;
            }

            if (!foundColumnFamily) {
                createColumnFamily(client, ksName, cfName, comparator);
            } else {
                log.debug("Keyspace {} and ColumnFamily {} were found.", ksName, cfName);
            }
        } catch (SchemaDisagreementException e) {
            throw new TemporaryBackendException(e);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        } finally {
            pool.returnObjectUnsafe(ksName, conn);
        }
    }

    private void createColumnFamily(Cassandra.Client client,
                                    String ksName,
                                    String cfName,
                                    String comparator) throws BackendException {

        CfDef createColumnFamily = new CfDef();
        createColumnFamily.setName(cfName);
        createColumnFamily.setKeyspace(ksName);
        createColumnFamily.setComparator_type(comparator);
        if (storageConfig.has(COMPACTION_STRATEGY)) {
            createColumnFamily.setCompaction_strategy(storageConfig.get(COMPACTION_STRATEGY));
        }
        if (!compactionOptions.isEmpty()) {
            createColumnFamily.setCompaction_strategy_options(compactionOptions);
        }
        ImmutableMap.Builder<String, String> compressionOptions = new ImmutableMap.Builder<String, String>();

        if (compressionEnabled) {
            compressionOptions.put("sstable_compression", compressionClass)
                    .put("chunk_length_kb", Integer.toString(compressionChunkSizeKB));
        }

        createColumnFamily.setCompression_options(compressionOptions.build());

        // Hard-coded caching settings
        if (cfName.startsWith(Backend.EDGESTORE_NAME)) {
            createColumnFamily.setCaching("keys_only");
        } else if (cfName.startsWith(Backend.INDEXSTORE_NAME)) {
            createColumnFamily.setCaching("rows_only");
        }

        log.debug("Adding column family {} to keyspace {}...", cfName, ksName);
        try {
            client.system_add_column_family(createColumnFamily);
        } catch (SchemaDisagreementException e) {
            throw new TemporaryBackendException("Error in setting up column family", e);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }

        log.debug("Added column family {} to keyspace {}.", cfName, ksName);
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws BackendException {
        CTConnection conn = null;
        Map<String, String> result = null;

        try {
            conn = pool.borrowObject(keySpaceName);
            Cassandra.Client client = conn.getClient();

            KsDef ksDef = client.describe_keyspace(keySpaceName);

            for (CfDef cfDef : ksDef.getCf_defs()) {
                if (null != cfDef && cfDef.getName().equals(cf)) {
                    result = cfDef.getCompression_options();
                    break;
                }
            }

            return result;
        } catch (InvalidRequestException e) {
            log.debug("Keyspace {} does not exist", keySpaceName);
            return null;
        } catch (Exception e) {
            throw new TemporaryBackendException(e);
        } finally {
            pool.returnObjectUnsafe(keySpaceName, conn);
        }
    }

    private void closePool() {
        /*
         * pool.close() does not affect borrowed connections.
         *
         * Connections currently borrowed by some thread which are
         * talking to the old host will eventually be destroyed by
         * CTConnectionFactory#validateObject() returning false when
         * those connections are returned to the pool.
         */
        try {
            pool.close();
            log.info("Closed Thrift connection pooler.");
        } catch (Exception e) {
            log.warn("Failed to close connection pooler.  "
                    + "We might be leaking Cassandra connections.", e);
            // There's still hope: CTConnectionFactory#validateObject()
            // will be called on borrow() and might tear down the
            // connections that close() failed to tear down
        }
    }
}

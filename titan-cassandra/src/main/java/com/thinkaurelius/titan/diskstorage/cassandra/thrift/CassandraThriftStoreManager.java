package com.thinkaurelius.titan.diskstorage.cassandra.thrift;

import static com.thinkaurelius.titan.diskstorage.cassandra.CassandraTransaction.getTx;
import static org.apache.cassandra.db.Table.SYSTEM_KS;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SchemaDisagreementException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.apache.thrift.TException;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.Backend;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnection;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionFactory.Config;
import com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool.CTConnectionPool;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.Hex;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * This class creates {@see CassandraThriftKeyColumnValueStore}s and
 * handles Cassandra-backed allocation of vertex IDs for Titan (when so
 * configured).
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraThriftStoreManager extends AbstractCassandraStoreManager {
    private static final Logger log = LoggerFactory.getLogger(CassandraThriftStoreManager.class);

    private final Map<String, CassandraThriftKeyColumnValueStore> openStores;
    private final CTConnectionPool pool;
    private final CTConnectionFactory factory;

    public CassandraThriftStoreManager(Configuration config) throws StorageException {
        super(config);

        String randomInitialHostname = getSingleHostname();

        int thriftTimeoutMS = config.getInt(
                GraphDatabaseConfiguration.CONNECTION_TIMEOUT_KEY,
                GraphDatabaseConfiguration.CONNECTION_TIMEOUT_DEFAULT);

        int maxTotalConnections = config.getInt(
                GraphDatabaseConfiguration.CONNECTION_POOL_SIZE_KEY,
                GraphDatabaseConfiguration.CONNECTION_POOL_SIZE_DEFAULT);

        this.factory = new CTConnectionFactory(randomInitialHostname, port,
                thriftTimeoutMS, thriftFrameSize);

        CTConnectionPool p = new CTConnectionPool(factory);
        p.setTestOnBorrow(true);
        p.setTestOnReturn(true);
        p.setTestWhileIdle(false);
        p.setWhenExhaustedAction(GenericKeyedObjectPool.WHEN_EXHAUSTED_BLOCK);
        p.setMaxActive(-1); // "A negative value indicates no limit"
        p.setMaxTotal(maxTotalConnections); // maxTotal limits active + idle
        p.setMinIdle(0); // prevent evictor from eagerly creating unused connections
        p.setMinEvictableIdleTimeMillis(60 * 1000L);
        p.setTimeBetweenEvictionRunsMillis(30 * 1000L);

        this.pool = p;

        this.openStores = new HashMap<String, CassandraThriftKeyColumnValueStore>();

        // Only watch the ring and change endpoints with BOP
        if (getCassandraPartitioner() instanceof ByteOrderedPartitioner) {
            this.backgroundThread = new Thread(new HostUpdater());
            this.backgroundThread.start();
        }
    }

    @Override
    public Partitioner getPartitioner() throws StorageException {
        return Partitioner.getPartitioner(getCassandraPartitioner());
    }

    @SuppressWarnings("unchecked")
    public IPartitioner<? extends Token<?>> getCassandraPartitioner() throws StorageException {
        CTConnection conn = null;
        try {
            conn = pool.borrowObject(SYSTEM_KS);
            return FBUtilities.newPartitioner(conn.getClient().describe_partitioner());
        } catch (Exception e) {
            throw new TemporaryStorageException(e);
        } finally {
            pool.returnObjectUnsafe(SYSTEM_KS, conn);
        }
    }

    @Override
    public String toString() {
        return "thriftCassandra" + super.toString();
    }

    @Override
    public void close() throws StorageException {
        openStores.clear();
        closePool();
        if (null != backgroundThread && backgroundThread.isAlive()) {
            backgroundThread.interrupt();
        }
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        Preconditions.checkNotNull(mutations);

        final Timestamp timestamp = getTimestamp(txh);

        ConsistencyLevel consistency = getTx(txh).getWriteConsistencyLevel().getThriftConsistency();

        // Generate Thrift-compatible batch_mutate() datastructure
        // key -> cf -> cassmutation
        int size = 0;
        for (Map<StaticBuffer, KCVMutation> mutation : mutations.values()) size += mutation.size();
        Map<ByteBuffer, Map<String, List<org.apache.cassandra.thrift.Mutation>>> batch =
                new HashMap<ByteBuffer, Map<String, List<org.apache.cassandra.thrift.Mutation>>>(size);


        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> keyMutation : mutations.entrySet()) {
            String columnFamily = keyMutation.getKey();
            for (Map.Entry<StaticBuffer, KCVMutation> mutEntry : keyMutation.getValue().entrySet()) {
                StaticBuffer key = mutEntry.getKey();
                ByteBuffer keyBB = key.asByteBuffer();

                // Get or create the single Cassandra Mutation object responsible for this key 
                Map<String, List<org.apache.cassandra.thrift.Mutation>> cfmutation = batch.get(keyBB);
                if (cfmutation == null) {
                    cfmutation = new HashMap<String, List<org.apache.cassandra.thrift.Mutation>>(3); // TODO where did the magic number 3 come from?
                    batch.put(keyBB, cfmutation);
                }

                KCVMutation mutation = mutEntry.getValue();
                List<org.apache.cassandra.thrift.Mutation> thriftMutation =
                        new ArrayList<org.apache.cassandra.thrift.Mutation>(mutations.size());

                if (mutation.hasDeletions()) {
                    for (StaticBuffer buf : mutation.getDeletions()) {
                        Deletion d = new Deletion();
                        SlicePredicate sp = new SlicePredicate();
                        sp.addToColumn_names(buf.asByteBuffer());
                        d.setPredicate(sp);
                        d.setTimestamp(timestamp.deletionTime);
                        org.apache.cassandra.thrift.Mutation m = new org.apache.cassandra.thrift.Mutation();
                        m.setDeletion(d);
                        thriftMutation.add(m);
                    }
                }

                if (mutation.hasAdditions()) {
                    for (Entry ent : mutation.getAdditions()) {
                        ColumnOrSuperColumn cosc = new ColumnOrSuperColumn();
                        Column column = new Column(ent.getColumn().asByteBuffer());
                        column.setValue(ent.getValue().asByteBuffer());
                        column.setTimestamp(timestamp.additionTime);
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
            client.batch_mutate(batch, consistency);
        } catch (Exception ex) {
            throw CassandraThriftKeyColumnValueStore.convertException(ex);
        } finally {
            pool.returnObjectUnsafe(keySpaceName, conn);
        }
    }

    @Override // TODO: *BIG FAT WARNING* 'synchronized is always *bad*, change openStores to use ConcurrentLinkedHashMap
    public synchronized CassandraThriftKeyColumnValueStore openDatabase(final String name) throws StorageException {
        if (openStores.containsKey(name))
            return openStores.get(name);

        ensureColumnFamilyExists(keySpaceName, name);

        CassandraThriftKeyColumnValueStore store = new CassandraThriftKeyColumnValueStore(keySpaceName, name, this, pool);
        openStores.put(name, store);
        return store;
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
     * @throws StorageException if any checked Thrift or UnknownHostException is thrown in the body of this method
     */
    public void clearStorage() throws StorageException {
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

            KsDef ksDef = null;
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
            throw new TemporaryStorageException(e);
        } finally {
            if (null != conn.getClient()) {
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

    private KsDef ensureKeyspaceExists(String keyspaceName)
            throws NotFoundException, InvalidRequestException, TException,
            SchemaDisagreementException, StorageException {

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
                        .setStrategy_class("org.apache.cassandra.locator.SimpleStrategy")
                        .setStrategy_options(ImmutableMap.of("replication_factor", String.valueOf(replicationFactor)));

                client.set_keyspace(SYSTEM_KS);
                try {
                    client.system_add_keyspace(ksdef);
                    log.debug("Created keyspace {}", keyspaceName);
                } catch (InvalidRequestException ire) {
                    log.error("system_add_keyspace failed for keyspace=" + keyspaceName, ire);
                    throw ire;
                }

            }

            return client.describe_keyspace(keyspaceName);
        } catch (Exception e) {
            throw new TemporaryStorageException(e);
        } finally {
            pool.returnObjectUnsafe(SYSTEM_KS, connection);
        }
    }

    private void ensureColumnFamilyExists(String ksName, String cfName) throws StorageException {
        ensureColumnFamilyExists(ksName, cfName, "org.apache.cassandra.db.marshal.BytesType");
    }

    private void ensureColumnFamilyExists(String ksName, String cfName, String comparator) throws StorageException {
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
            throw new TemporaryStorageException(e);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            pool.returnObjectUnsafe(ksName, conn);
        }
    }

    private void createColumnFamily(Cassandra.Client client,
                                           String ksName,
                                           String cfName,
                                           String comparator) throws StorageException {

        CfDef createColumnFamily = new CfDef();
        createColumnFamily.setName(cfName);
        createColumnFamily.setKeyspace(ksName);
        createColumnFamily.setComparator_type(comparator);
        
        ImmutableMap.Builder<String, String> compressionOptions = new ImmutableMap.Builder<String, String>();

        if (compressionEnabled) {
            compressionOptions.put("sstable_compression", compressionClass)
                              .put("chunk_length_kb", Integer.toString(compressionChunkSizeKB));
        }

        createColumnFamily.setCompression_options(compressionOptions.build());

        // Hard-coded caching settings
        if (cfName.startsWith(Backend.EDGESTORE_NAME)) {
            createColumnFamily.setCaching("keys_only");
        } else if (cfName.startsWith(Backend.VERTEXINDEX_STORE_NAME)) {
            createColumnFamily.setCaching("rows_only");
        }

        log.debug("Adding column family {} to keyspace {}...", cfName, ksName);
        try {
            client.system_add_column_family(createColumnFamily);
        } catch (SchemaDisagreementException e) {
            throw new TemporaryStorageException("Error in setting up column family", e);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        }

        log.debug("Added column family {} to keyspace {}.", cfName, ksName);
    }

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        CTConnection connection = null;

        try {
            ensureColumnFamilyExists(keySpaceName, SYSTEM_PROPERTIES_CF, "org.apache.cassandra.db.marshal.UTF8Type");

            connection = pool.borrowObject(keySpaceName);
            Cassandra.Client client = connection.getClient();
            ColumnOrSuperColumn column = client.get(UTF8Type.instance.fromString(SYSTEM_PROPERTIES_KEY),
                    new ColumnPath(SYSTEM_PROPERTIES_CF).setColumn(UTF8Type.instance.fromString(key)),
                    ConsistencyLevel.QUORUM);

            if (column == null || !column.isSetColumn())
                return null;

            Column actualColumn = column.getColumn();

            return (actualColumn.value == null)
                    ? null
                    : UTF8Type.instance.getString(actualColumn.value);
        } catch (NotFoundException e) {
            return null;
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            pool.returnObjectUnsafe(keySpaceName, connection);
        }
    }

    @Override
    public void setConfigurationProperty(final String rawKey, final String rawValue) throws StorageException {
        CTConnection connection = null;

        try {
            connection = pool.borrowObject(keySpaceName);

            ensureColumnFamilyExists(keySpaceName, SYSTEM_PROPERTIES_CF, "org.apache.cassandra.db.marshal.UTF8Type");

            ByteBuffer key = UTF8Type.instance.fromString(rawKey);
            ByteBuffer val = UTF8Type.instance.fromString(rawValue);

            Cassandra.Client client = connection.getClient();

            client.insert(UTF8Type.instance.fromString(SYSTEM_PROPERTIES_KEY),
                    new ColumnParent(SYSTEM_PROPERTIES_CF),
                    new Column(key).setValue(val).setTimestamp(System.currentTimeMillis()),
                    ConsistencyLevel.QUORUM);
        } catch (Exception e) {
            throw new PermanentStorageException(e);
        } finally {
            pool.returnObjectUnsafe(keySpaceName, connection);
        }
    }

    @Override
    public Map<String, String> getCompressionOptions(String cf) throws StorageException {
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
            throw new TemporaryStorageException(e);
        } finally {
            pool.returnObjectUnsafe(keySpaceName, conn);
        }
    }


    private static final double DECAY_EXPONENT_MULTI = 0.0005;
    private static final int DEFAULT_HOST_UPDATE_INTERVAL_MS = 10000;

    private Thread backgroundThread = null;

    private final NonBlockingHashMap<ByteBuffer, Counter> countsByEndToken = new NonBlockingHashMap<ByteBuffer, Counter>();
    private volatile ImmutableMap<ByteBuffer, String> nodesByEndToken = ImmutableMap.of();

    public String getKeyHostname(ByteBuffer key) {

        ImmutableMap<ByteBuffer, String> tokenMap = nodesByEndToken;

        ByteBuffer bb = getKeyEndToken(key, tokenMap);

        return null == bb ? null : tokenMap.get(bb);
    }

    public ByteBuffer getKeyEndToken(ByteBuffer key) {

        ImmutableMap<ByteBuffer, String> tokenMap = nodesByEndToken;

        return getKeyEndToken(key, tokenMap);
    }

    private ByteBuffer getKeyEndToken(ByteBuffer key,
                                      ImmutableMap<ByteBuffer, String> tokenMap) {

        if (0 == tokenMap.size()) {
            return null;
        }

        ByteBuffer lastToken = null;
        ByteBuffer result = null;

        for (ByteBuffer curToken : tokenMap.keySet()) {
            if (null == lastToken) { // Special case for first iteration
                if (ByteBufferUtil.isSmallerOrEqualThan(key, curToken)) {
                    // This key is part of the "wrapping range"
                    result = curToken;

                    if (log.isTraceEnabled()) {
                        log.trace("Key {} precedes or equals first token {}",
                                ByteBufferUtil.bytesToHex(key),
                                ByteBufferUtil.bytesToHex(curToken));
                    }

                    break;
                }
            } else if (ByteBufferUtil.isSmallerOrEqualThan(key, curToken)
                    && ByteBufferUtil.isSmallerThan(lastToken, key)) { // General case
                result = curToken;

                if (log.isTraceEnabled()) {
                    log.trace("Key {} falls between tokens {} and {}",
                            new Object[]{ByteBufferUtil.bytesToHex(key),
                                    ByteBufferUtil.bytesToHex(lastToken),
                                    ByteBufferUtil.bytesToHex(curToken)});
                }

                break;
            }

            lastToken = curToken;
        }

        // Wrapping range case 2: key is greater than all tokens
        if (result == null) {
            assert 0 < tokenMap.size();
            assert null != lastToken;

            result = tokenMap.keySet().iterator().next();

            assert null != result;

            if (log.isTraceEnabled()) {
                log.trace("Key {} succeeds all tokens (last token was {}); "
                        + "assigning it to initial token {}",
                        new Object[]{ByteBufferUtil.bytesToHex(key),
                                ByteBufferUtil.bytesToHex(lastToken),
                                ByteBufferUtil.bytesToHex(result)});
            }
        }

        return result;
    }

    public void updateCounter(ByteBuffer key) {
        ByteBuffer token = getKeyEndToken(key);

        if (null == token) {
            if (log.isDebugEnabled()) {
                log.debug(
                        "No token found for key {} (skipping counter update)",
                        ByteBufferUtil.bytesToHex(key));
            }
            return;
        }

        Counter c = countsByEndToken.get(token);
        if (null == c) {
            countsByEndToken.putIfAbsent(token, new Counter());
            c = countsByEndToken.get(token);
        }
        assert null != c;

        c.update();

        if (log.isTraceEnabled()) {
            log.trace("Updated counter for token {} (responsible for key {})",
                    ByteBufferUtil.bytesToHex(token),
                    ByteBufferUtil.bytesToHex(key));
        }
    }

    private void updatePools() throws InterruptedException {

        ByteBuffer hottestEndToken = null;
        double hottestEndTokenValue = 0D;
        
        /*
         * Count the number of iterations over countsByEndToken.entrySet()'s
         * members and store the result in perceivedEntrySetSize. We do this
         * instead of just calling countsByEntrySet.size() later because the map
         * contents and size could change between the invocation of entrySet()
         * and the later line of code where we want to know how large the entry
         * set was.
         */
        int perceivedEntrySetSize = 0;

        for (Map.Entry<ByteBuffer, Counter> entry : countsByEndToken.entrySet()) {
            if (hottestEndToken == null
                    || hottestEndTokenValue < entry.getValue().currentValue()) {
                hottestEndToken = entry.getKey();
                hottestEndTokenValue = entry.getValue().currentValue();
            }
            perceivedEntrySetSize++;
        }

        // Talk directly to the first replica responsible for the hot token
        if (null != hottestEndToken) {

            String hotHost = getKeyHostname(hottestEndToken);
            assert null != hotHost;

            assert null != factory;
            Config cfg = factory.getConfig();
            assert null != cfg;

            String curHost = cfg.getHostname();
            assert null != curHost;

            if (curHost.equals(hotHost)) {
                log.info(
                        "Already connected to hottest Cassandra endpoint: {} with hotness {}",
                        hotHost, hottestEndTokenValue);
            } else {
                log.info(
                        "New hottest Cassandra endpoint found: {} with hotness {}",
                        hotHost, hottestEndTokenValue);
                Config newConfig = new Config(hotHost, cfg.getPort(),
                        cfg.getTimeoutMS(), cfg.getFrameSize());

                assert !newConfig.equals(cfg);

                /*
                 * Destroy idle connections to the old host. This step is not
                 * strictly necessary so long as pool#getTestOnBorrow() is true.
                 * This step just eagerly destroys the idle connections, whereas
                 * without this step the pool would detect and destroy
                 * connections to the old host lazily (one-by-one as the pool
                 * handled borrow requests).
                 */
                pool.clear();
                // Set the new hostname on the pool's connection factory
                factory.setConfig(newConfig);

                log.info("Directing all future Cassandra ops to {}", hotHost);
            }
        } else {
            log.debug("No hottest end token found.  countsByEndToken size={}",
                    perceivedEntrySetSize);
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

    /**
     * Refresh instance variables about the Cassandra ring so that
     * #getKeyHostname and #getKeyEndToken return up-to-date information.
     */
    private void updateRing() {

        CTConnection conn = null;

        try {
            conn = pool.borrowObject(SYSTEM_KS);

            Map<String, String> tm = conn.getClient().describe_token_map();

            Pattern oldPat = Pattern.compile("Token\\(bytes\\[(.+)\\]\\)");
            Pattern newbytesPat = Pattern.compile("^([0-9a-fA-F]+)$");

            // Build a temporary TreeMap of ordered tokens and their replica IPs
            SortedMap<ByteBuffer, String> sortedMap =
                    new TreeMap<ByteBuffer, String>(new Comparator<ByteBuffer>() {

                        @Override
                        public int compare(ByteBuffer a, ByteBuffer b) {
                            return ByteBufferUtil.compare(a, b);
                        }

                    });

            for (Map.Entry<String, String> ent : tm.entrySet()) {
                // The raw token string has the form "Token(bytes[8000000000000000])"
                // Strip off the Token(bytes[]) part
                String rawToken = ent.getKey();
                Matcher m = oldPat.matcher(rawToken);
                if (!m.matches()) {
                    m = newbytesPat.matcher(rawToken);
                }
                if (!m.matches()) {
                    log.error("Couldn't match token {} against pattern {} or {} ", new Object[] { rawToken, oldPat, newbytesPat });
                    pool.returnObjectUnsafe(SYSTEM_KS, conn);
                    conn = null;
                    return;
                }
                Preconditions.checkArgument(m.matches());
                String token = m.group(1);
                String nodeIP = ent.getValue();

                ByteBuffer tokenBB = ByteBuffer.wrap(Hex.hexToBytes(token));

                sortedMap.put(tokenBB, nodeIP);
            }

            // Copy temporary sorted-by-token map to an ImmutableMap
            // and publish the ImmutableMap to a volatile reference
            nodesByEndToken = ImmutableMap.copyOf(sortedMap);

            if (log.isDebugEnabled()) {
                log.debug("Updated Cassandra TokenMap:");
                for (Map.Entry<ByteBuffer, String> ent : nodesByEndToken.entrySet()) {
                    log.debug("TokenMap entry: {} -> {}",
                            ByteBufferUtil.bytesToHex(ent.getKey()),
                            ent.getValue());
                }
                log.debug("End of Cassandra TokenMap.");
            }
        } catch (StorageException e) {
            log.error("Failed to acquire pooled Cassandra connection", e);
            // Don't propagate exception
        } catch (InvalidRequestException e) {
            log.error("Failed to describe Cassandra token map", e);
            // Don't propagate exception
        } catch (TException e) {
            log.error("Thrift Exception while getting Cassandra token map", e);
            // Don't propagate exception
        } catch (Exception e) {
            log.error("Unknown Exception while getting Cassandra token map", e);
            // Don't propagate exception
        } finally {
            if (null != conn)
                pool.returnObjectUnsafe(SYSTEM_KS, conn);
        }
    }

    private class HostUpdater implements Runnable {

        private long lastUpdateTime;
        private final long updateInterval;

        public HostUpdater() {
            this(DEFAULT_HOST_UPDATE_INTERVAL_MS);
        }

        public HostUpdater(final long updateInterval) {
            Preconditions.checkArgument(updateInterval > 0);
            this.updateInterval = updateInterval;
            lastUpdateTime = System.currentTimeMillis();
        }

        @Override
        public void run() {
            while (true) {
                long sleepTime = updateInterval
                        - (System.currentTimeMillis() - lastUpdateTime);
                try {
                    Thread.sleep(Math.max(0, sleepTime));
                    updateRing();
                    updatePools();
                    lastUpdateTime = System.currentTimeMillis();
                    log.debug("HostUpdater lastUpdateTime={}", lastUpdateTime);
                } catch (InterruptedException e) {
                    log.info("Background update thread shutting down...");
                    return;
                }
            }
        }
    }

    private static class Counter {

        private double value = 0.0;
        private long lastUpdate = 0;

        public synchronized void update() {
            value = currentValue() + 1.0;
            lastUpdate = System.currentTimeMillis();
        }

        public synchronized double currentValue() {
            return value
                    * Math.exp(-DECAY_EXPONENT_MULTI
                    * (System.currentTimeMillis() - lastUpdate));
        }
    }
}

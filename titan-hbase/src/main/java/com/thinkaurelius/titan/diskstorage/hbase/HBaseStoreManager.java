package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionConfig;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.util.Timestamps;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import com.thinkaurelius.titan.util.system.NetworkUtil;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.thinkaurelius.titan.diskstorage.Backend.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;

/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStoreManager.class);

    public static final ConfigOption<String> HBASE_TABLE = new ConfigOption<String>(STORAGE_NS,"tablename",
            "The name of the table to store Titan's data in",
            ConfigOption.Type.LOCAL, "titan");

    public static final ConfigOption<Boolean> SHORT_CF_NAMES = new ConfigOption<Boolean>(STORAGE_NS,"short-cf-names",
            "Whether to automatically shorten the names of frequently used column families to preserve space",
            ConfigOption.Type.FIXED, true);

//    public static final String SHORT_CF_NAMES_KEY = "short-cf-names";
//    public static final boolean SHORT_CF_NAMES_DEFAULT = false;

    public static final int PORT_DEFAULT = 9160;

    public static final ConfigNamespace HBASE_CONFIGURATION_NAMESPACE = new ConfigNamespace(STORAGE_NS,"hbase-config","General HBase configuration options",true);

    public static final ImmutableMap<ConfigOption, String> HBASE_CONFIGURATION = ImmutableMap.of(
            (ConfigOption)GraphDatabaseConfiguration.STORAGE_HOSTS, "hbase.zookeeper.quorum",
            GraphDatabaseConfiguration.PORT, "hbase.zookeeper.property.clientPort"
    );

    private final String tableName;
    private final org.apache.hadoop.conf.Configuration hconf;

    private final ConcurrentMap<String, HBaseKeyColumnValueStore> openStores;
    private final HTablePool connectionPool;

    private final boolean shortCfNames;
    private static final BiMap<String, String> shortCfNameMap =
            ImmutableBiMap.<String, String>builder()
                    .put(VERTEXINDEX_STORE_NAME, "v")
                    .put(ID_STORE_NAME, "i")
                    .put(EDGESTORE_NAME, "s")
                    .put(EDGEINDEX_STORE_NAME, "e")
                    .put(VERTEXINDEX_STORE_NAME + LOCK_STORE_SUFFIX, "w")
                    .put(ID_STORE_NAME + LOCK_STORE_SUFFIX, "j")
                    .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "t")
                    .put(EDGEINDEX_STORE_NAME + LOCK_STORE_SUFFIX, "f")
                    .build();

    static {
        // Verify that shortCfNameMap is injective
        // Should be guaranteed by Guava BiMap, but it doesn't hurt to check
        Preconditions.checkArgument(null != shortCfNameMap);
        Collection<String> shorts = shortCfNameMap.values();
        Preconditions.checkArgument(Sets.newHashSet(shorts).size() == shorts.size());
    }

    public HBaseStoreManager(com.thinkaurelius.titan.diskstorage.configuration.Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        this.tableName = config.get(HBASE_TABLE);

        this.hconf = HBaseConfiguration.create();
        for (Map.Entry<ConfigOption, String> confEntry : HBASE_CONFIGURATION.entrySet()) {
            if (config.has(confEntry.getKey())) {
                hconf.set(confEntry.getValue(), config.get(confEntry.getKey()).toString());
            }
        }

        // Copy a subset of our commons config into a Hadoop config
        int keysLoaded=0;
        Map<String,Object> configSub = config.getSubset(HBASE_CONFIGURATION_NAMESPACE);
        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
            logger.debug("HBase configuration: setting {}={}", entry.getKey(), entry.getValue());
            if (entry.getValue()==null) continue;
            hconf.set(entry.getKey(), entry.getValue().toString());
            keysLoaded++;
        }

        logger.debug("HBase configuration: set a total of {} configuration values", keysLoaded);

        connectionPool = new HTablePool(hconf, connectionPoolSize);

        this.shortCfNames = config.get(SHORT_CF_NAMES);

        openStores = new ConcurrentHashMap<String, HBaseKeyColumnValueStore>();
    }

    @Override
    public Deployment getDeployment() {
        List<KeyRange> local = getLocalKeyPartition();
        return null != local && !local.isEmpty() ? Deployment.LOCAL : Deployment.REMOTE;
    }

    @Override
    public String toString() {
        return "hbase[" + tableName + "@" + super.toString() + "]";
    }

    @Override
    public void close() {
        openStores.clear();
    }


    @Override
    public StoreFeatures getFeatures() {
        // StoreFeatures features = new StoreFeatures();
        // features.supportsOrderedScan = true;
        // features.supportsUnorderedScan = true;
        // features.supportsBatchMutation = true;
        // features.supportsTxIsolation = false;
        // features.supportsMultiQuery = true;
        // features.supportsConsistentKeyOperations = true;
        // features.supportsLocking = false;
        // features.isKeyOrdered = true;
        // features.isDistributed = true;
        // features.hasLocalKeyPartition = false;
        // try {
        // features.hasLocalKeyPartition = getDeployment()==Deployment.LOCAL;
        // } catch (Exception e) {
        // logger.warn("Unexpected exception during getDeployment()", e);
        // }
        // return features;

        Configuration c = GraphDatabaseConfiguration.buildConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
                .orderedScan(true).unorderedScan(true).batchMutation(true)
                .multiQuery(true).distributed(true).keyOrdered(true)
                .keyConsistent(c);

        try {
            fb.localKeyPartition(getDeployment() == Deployment.LOCAL);
        } catch (Exception e) {
            logger.warn("Unexpected exception during getDeployment()", e);
        }

        return fb.build();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        final Timestamp timestamp = super.getTimestamp(txh);
        // In case of an addition and deletion with identical timestamps, the
        // deletion tombstone wins.
        // http://hbase.apache.org/book/versions.html#d244e4250
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = convertToCommands(mutations, timestamp.additionTime, timestamp.deletionTime);

        List<Row> batch = new ArrayList<Row>(commandsPerKey.size()); // actual batch operation

        // convert sorted commands into representation required for 'batch' operation
        for (Pair<Put, Delete> commands : commandsPerKey.values()) {
            if (commands.getFirst() != null)
                batch.add(commands.getFirst());

            if (commands.getSecond() != null)
                batch.add(commands.getSecond());
        }

        try {
            HTableInterface table = null;

            try {
                table = connectionPool.getTable(tableName);
                table.batch(batch);
                table.flushCommits();
            } finally {
                IOUtils.closeQuietly(table);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } catch (InterruptedException e) {
            throw new TemporaryStorageException(e);
        }

        sleepAfterWrite(txh);
    }

    @Override
    public KeyColumnValueStore openDatabase(final String longName) throws StorageException {

        HBaseKeyColumnValueStore store = openStores.get(longName);

        if (store == null) {

            final String cfName = shortCfNames ? shortenCfName(longName) : longName;

            HBaseKeyColumnValueStore newStore = new HBaseKeyColumnValueStore(this, connectionPool, tableName, cfName, longName);

            store = openStores.putIfAbsent(longName, newStore); // nothing bad happens if we loose to other thread

            if (store == null) { // ensure that CF exists only first time somebody tries to open it
                ensureColumnFamilyExists(tableName, cfName);
                store = newStore;
            }
        }

        return store;
    }

    List<KeyRange> getLocalKeyPartition() {

        List<KeyRange> result = new LinkedList<KeyRange>();

        HTable table = null;
        try {
            table = new HTable(hconf, tableName);
            NavigableMap<HRegionInfo, ServerName> regionLocs =
                    table.getRegionLocations();

            for (Map.Entry<HRegionInfo, ServerName> e : regionLocs.entrySet()) {
                if (NetworkUtil.isLocalConnection(e.getValue().getHostname())) {
                    HRegionInfo regionInfo = e.getKey();
                    byte startKey[] = regionInfo.getStartKey();
                    byte endKey[]   = regionInfo.getEndKey();

                    StaticBuffer startBuf = StaticArrayBuffer.of(startKey);
                    StaticBuffer endBuf =
                            StaticArrayBuffer.of(ByteBufferUtil.nextBiggerBufferAllowOverflow(ByteBuffer.wrap(endKey)));

                    KeyRange kr = new KeyRange(startBuf, endBuf);

                    result.add(kr);

                    logger.debug("Found local key/row partition {} on host {}", kr, e.getValue());
                } else {
                    logger.debug("Discarding remote {}", e.getValue());
                }
            }
        } catch (MasterNotRunningException e) {
            logger.warn("Unexpected MasterNotRunningException", e);
        } catch (ZooKeeperConnectionException e) {
            logger.warn("Unexpected ZooKeeperConnectionException", e);
        } catch (IOException e) {
            logger.warn("Unexpected IOException", e);
        } finally {
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.warn("Failed to close HTable {}", table, e);
                }
            }
        }

        return result;
    }

    private String shortenCfName(String longName) throws PermanentStorageException {
        final String s;
        if (shortCfNameMap.containsKey(longName)) {
            s = shortCfNameMap.get(longName);
            Preconditions.checkNotNull(s);
            logger.debug("Substituted default CF name \"{}\" with short form \"{}\" to reduce HBase KeyValue size", longName, s);
        } else {
            if (shortCfNameMap.containsValue(longName)) {
                String fmt = "Must use CF long-form name \"%s\" instead of the short-form name \"%s\" when configured with %s=true";
                String msg = String.format(fmt, shortCfNameMap.inverse().get(longName), longName, SHORT_CF_NAMES.getName());
                throw new PermanentStorageException(msg);
            }
            s = longName;
            logger.debug("Kept default CF name \"{}\" because it has no associated short form", s);
        }
        return s;
    }

    private HTableDescriptor ensureTableExists(String tableName) throws StorageException {
        HBaseAdmin adm = getAdminInterface();

        HTableDescriptor desc;

        try { // Create our table, if necessary
            if (adm.tableExists(tableName)) {
                desc = adm.getTableDescriptor(tableName.getBytes());
            } else {
                desc = new HTableDescriptor(tableName);
                adm.createTable(desc);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }

        return desc;
    }

    private void ensureColumnFamilyExists(String tableName, String columnFamily) throws StorageException {
        HBaseAdmin adm = getAdminInterface();
        HTableDescriptor desc = ensureTableExists(tableName);

        Preconditions.checkNotNull(desc);

        HColumnDescriptor cf = desc.getFamily(columnFamily.getBytes());

        // Create our column family, if necessary
        if (cf == null) {
            try {
                adm.disableTable(tableName);
                desc.addFamily(new HColumnDescriptor(columnFamily).setCompressionType(Compression.Algorithm.GZ));
                adm.modifyTable(tableName.getBytes(), desc);

                try {
                    logger.debug("Added HBase ColumnFamily {}, waiting for 1 sec. to propogate.", columnFamily);
                    Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                    throw new TemporaryStorageException(ie);
                }

                adm.enableTable(tableName);
            } catch (TableNotFoundException ee) {
                logger.error("TableNotFoundException", ee);
                throw new PermanentStorageException(ee);
            } catch (org.apache.hadoop.hbase.TableExistsException ee) {
                logger.debug("Swallowing exception {}", ee);
            } catch (IOException ee) {
                throw new TemporaryStorageException(ee);
            }
        } else { // check if compression was enabled, if not - enable it
            if (cf.getCompressionType() == null || cf.getCompressionType() == Compression.Algorithm.NONE) {
                try {
                    adm.disableTable(tableName);

                    adm.modifyColumn(tableName, cf.setCompressionType(Compression.Algorithm.GZ));

                    adm.enableTable(tableName);
                } catch (IOException e) {
                    throw new TemporaryStorageException(e);
                }
            }
        }
    }

    @Override
    public StoreTransaction beginTransaction(final TransactionHandleConfig config) throws StorageException {
        return new HBaseTransaction(config);
    }

    /**
     * Deletes the specified table with all its columns.
     * ATTENTION: Invoking this method will delete the table if it exists and therefore causes data loss.
     */
    @Override
    public void clearStorage() throws StorageException {
        HBaseAdmin adm = getAdminInterface();

        try { // first of all, check if table exists, if not - we are done
            if (!adm.tableExists(tableName)) {
                logger.debug("clearStorage() called before table {} was created, skipping.", tableName);
                return;
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }

        /*
         * The commented code is the recommended way to truncate an HBase table.
         * But it's so slow. The titan-hbase test suite takes 18 minutes to
         * complete on my machine using the Scanner method. It takes 1 hour 17
         * minutes to complete using the disable-delete-and-recreate method
         * commented below. (after - before) below is usually between 3000 and
         * 3100 ms on my machine, but it runs so many times in the test suite
         * that it adds up.
         */
//        long before = System.currentTimeMillis();
//        try {
//            adm.disableTable(tableName);
//            adm.deleteTable(tableName);
//        } catch (IOException e) {
//            throw new PermanentStorageException(e);
//        }
//        ensureTableExists(tableName);
//        long after = System.currentTimeMillis();
//        logger.debug("Dropped and recreated table {} in {} ms", tableName, after - before);

        HTable table = null;

        try {
            table = new HTable(hconf, tableName);

            Scan scan = new Scan();
            scan.setBatch(100);
            scan.setCacheBlocks(false);
            scan.setCaching(2000);
            scan.setTimeRange(0, Long.MAX_VALUE);
            scan.setMaxVersions(1);

            ResultScanner scanner = null;

            long ts = -1;

            try {
                scanner = table.getScanner(scan);

                for (Result res : scanner) {
                    Delete d = new Delete(res.getRow());
                    //Despite comment in Delete.java, LATEST_TIMESTAMP seems to be System.currentTimeMillis()
                    //LATEST_TIMESTAMP is the default for the constructor invoked above, so it's redundant anyway
                    //d.setTimestamp(HConstants.LATEST_TIMESTAMP);

                    if (-1 == ts)
                        ts = guessTimestamp(res);

                    d.setTimestamp(ts);
                    table.delete(d);
                }
            } finally {
                IOUtils.closeQuietly(scanner);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    private static long guessTimestamp(Result res) {

        Long sampleTime = res.getMap().firstEntry().getValue().firstEntry().getValue().firstEntry().getKey();
        // Estimate timestamp unit from order of magnitude assuming UNIX epoch -- not compatible with arbitrary custom timestamps
        Preconditions.checkArgument(null != sampleTime);
        final double exponent = Math.log10(sampleTime);
        final TimestampProvider prov;

        /*
         * These exponent brackets approximately cover UNIX Epoch timestamps
         * between:
         *
         * Sat Sep 8 21:46:40 EDT 2001
         *
         * Thu Sep 26 21:46:40 EDT 33658
         *
         * Even though it won't rollover, this timestamp guessing kludge still
         * eventually be refactored away to support arbitrary timestamps
         * provided by the user. There's no good reason clearStorage() should be
         * timestamp sensitive, it's just that truncating tables in the way
         * recommended by HBase is so incredibly slow that it more than doubles
         * the walltime taken by the titan-hbase test suite.
         */
        if (12 <= exponent && exponent < 15)
            prov = Timestamps.MILLI;
        else if (15 <= exponent && exponent < 18)
            prov = Timestamps.MICRO;
        else if (18 <= exponent && exponent < 21)
            prov = Timestamps.NANO;
        else
            throw new IllegalStateException("Timestamp " + sampleTime + " does not match expected UNIX Epoch timestamp in milli-, micro-, or nanosecond units.  clearStorage() does not support custom timestamps.");

        logger.debug("Guessed timestamp provider " + prov);

        return prov.getTime();


    }

    @Override
    public String getName() {
        return tableName;
    }

    private HBaseAdmin getAdminInterface() {
        try {
            return new HBaseAdmin(hconf);
        } catch (IOException e) {
            throw new TitanException(e);
        }
    }

    /**
     * Convert Titan internal Mutation representation into HBase native commands.
     *
     * @param mutations    Mutations to convert into HBase commands.
     * @param putTimestamp The timestamp to use for Put commands.
     * @param delTimestamp The timestamp to use for Delete commands.
     * @return Commands sorted by key converted from Titan internal representation.
     * @throws PermanentStorageException
     */
    private Map<StaticBuffer, Pair<Put, Delete>> convertToCommands(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
                                                                   final long putTimestamp,
                                                                   final long delTimestamp) throws PermanentStorageException {
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = new HashMap<StaticBuffer, Pair<Put, Delete>>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {

            String cfString = getCfNameForStoreName(entry.getKey());
            byte[] cfName = cfString.getBytes();

            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                byte[] key = m.getKey().as(StaticBuffer.ARRAY_FACTORY);
                KCVMutation mutation = m.getValue();

                Pair<Put, Delete> commands = commandsPerKey.get(m.getKey());

                if (commands == null) {
                    commands = new Pair<Put, Delete>();
                    commandsPerKey.put(m.getKey(), commands);
                }

                if (mutation.hasDeletions()) {
                    if (commands.getSecond() == null) {
                        Delete d = new Delete(key);
                        d.setTimestamp(delTimestamp);
                        commands.setSecond(d);
                    }

                    for (StaticBuffer b : mutation.getDeletions()) {
                        commands.getSecond().deleteColumns(cfName, b.as(StaticBuffer.ARRAY_FACTORY), delTimestamp);
                    }
                }

                if (mutation.hasAdditions()) {
                    if (commands.getFirst() == null) {
                        Put p = new Put(key, putTimestamp);
                        commands.setFirst(p);
                    }

                    for (Entry e : mutation.getAdditions()) {
                        commands.getFirst().add(cfName,
                                e.getColumnAs(StaticBuffer.ARRAY_FACTORY),
                                putTimestamp,
                                e.getValueAs(StaticBuffer.ARRAY_FACTORY));
                    }
                }
            }
        }

        return commandsPerKey;
    }

    private String getCfNameForStoreName(String storeName) throws PermanentStorageException {
        return shortCfNames ? shortenCfName(storeName) : storeName;
    }
}

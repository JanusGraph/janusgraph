package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.util.time.TimestampProvider;
import com.thinkaurelius.titan.util.time.Timestamps;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import com.thinkaurelius.titan.util.system.NetworkUtil;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.thinkaurelius.titan.diskstorage.Backend.*;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_NS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;

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

    public static final String COMPRESSION_DEFAULT = "-DEFAULT-";

    public static final ConfigOption<String> COMPRESSION = new ConfigOption<String>(STORAGE_NS,"compression-algorithm",
            "An HBase Compression.Algorithm enum string which will be applied to newly created column families",
            ConfigOption.Type.MASKABLE, "GZ");

    /**
     * Related bug fixed in 0.98.0, 0.94.7, 0.95.0:
     *
     * https://issues.apache.org/jira/browse/HBASE-8170
     */
    public static final int MIN_REGION_COUNT = 3;

    public static final ConfigOption<Boolean> SKIP_SCHEMA_CHECK =  new ConfigOption<Boolean>(STORAGE_NS,"skip-schema-check",
            "Assume that Titan's HBase table and column families already exist",
            ConfigOption.Type.MASKABLE, false);

    /**
     * The total number of HBase regions to create with Titan's table. This
     * setting only effects table creation; this normally happens just once when
     * Titan connects to an HBase backend for the first time.
     */
    public static final ConfigOption<Integer> REGION_COUNT = new ConfigOption<Integer>(STORAGE_NS, "region-count",
            "The number of initial regions set when creating Titan's HBase table",
            ConfigOption.Type.MASKABLE, Integer.class, new Predicate<Integer>() {
                @Override
                public boolean apply(Integer input) {
                    return null != input && MIN_REGION_COUNT <= input;
                }
            }
    );

    /**
     * This setting is used only when {@link #REGION_COUNT} is unset.
     * <p/>
     * If Titan's HBase table does not exist, then it will be created with total
     * region count = (number of servers reported by ClusterStatus) * (this
     * value).
     * <p/>
     * The Apache HBase manual suggests an order-of-magnitude range of potential
     * values for this setting:
     *
     * <ul>
     *  <li>
     *   <a href="https://hbase.apache.org/book/important_configurations.html#disable.splitting">2.5.2.7. Managed Splitting</a>:
     *   <blockquote>
     *    What's the optimal number of pre-split regions to create? Mileage will
     *    vary depending upon your application. You could start low with 10
     *    pre-split regions / server and watch as data grows over time. It's
     *    better to err on the side of too little regions and rolling split later.
     *   </blockquote>
     *  </li>
     *  <li>
     *   <a href="https://hbase.apache.org/book/regions.arch.html">9.7 Regions</a>:
     *   <blockquote>
     *    In general, HBase is designed to run with a small (20-200) number of
     *    relatively large (5-20Gb) regions per server... Typically you want to
     *    keep your region count low on HBase for numerous reasons. Usually
     *    right around 100 regions per RegionServer has yielded the best results.
     *   </blockquote>
     *  </li>
     * </ul>
     *
     * These considerations may differ for other HBase implementations (e.g. MapR).
     */
    public static final ConfigOption<Integer> REGIONS_PER_SERVER = new ConfigOption<Integer>(STORAGE_NS, "regions-per-server",
            "The number of regions per regionserver to set when creating Titan's HBase table",
            ConfigOption.Type.MASKABLE, Integer.class);

    public static final int PORT_DEFAULT = 9160;

    public static final ConfigNamespace HBASE_CONFIGURATION_NAMESPACE =
            new ConfigNamespace(STORAGE_NS,"hbase-config","General HBase configuration options",true);

    private static final BiMap<String, String> SHORT_CF_NAME_MAP =
            ImmutableBiMap.<String, String>builder()
                    .put(INDEXSTORE_NAME, "v")
                    .put(ID_STORE_NAME, "i")
                    .put(EDGESTORE_NAME, "s")
                    .put(INDEXSTORE_NAME + LOCK_STORE_SUFFIX, "w")
                    .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "t")
                    .put(SYSTEM_PROPERTIES_STORE_NAME, "c")
                    .build();

    private static final StaticBuffer FOUR_ZERO_BYTES = BufferUtil.zeroBuffer(4);

    static {
        // Verify that shortCfNameMap is injective
        // Should be guaranteed by Guava BiMap, but it doesn't hurt to check
        Preconditions.checkArgument(null != SHORT_CF_NAME_MAP);
        Collection<String> shorts = SHORT_CF_NAME_MAP.values();
        Preconditions.checkArgument(Sets.newHashSet(shorts).size() == shorts.size());
    }

    // Immutable instance fields
    private final String tableName;
    private final String compression;
    private final int regionCount;
    private final int regionsPerServer;
    private final HConnection cnx;
    private final org.apache.hadoop.conf.Configuration hconf;
    private final boolean shortCfNames;
    private final boolean skipSchemaCheck;

    // Mutable instance state
    private final ConcurrentMap<String, HBaseKeyColumnValueStore> openStores;

    public HBaseStoreManager(com.thinkaurelius.titan.diskstorage.configuration.Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        checkConfigDeprecation(config);

        this.tableName = config.get(HBASE_TABLE);
        this.compression = config.get(COMPRESSION);
        this.regionCount = config.has(REGION_COUNT) ? config.get(REGION_COUNT) : -1;
        this.regionsPerServer = config.has(REGIONS_PER_SERVER) ? config.get(REGIONS_PER_SERVER) : -1;
        this.skipSchemaCheck = config.get(SKIP_SCHEMA_CHECK);

        /*
         * Specifying both region count options is permitted but may be
         * indicative of a misunderstanding, so issue a warning.
         */
        if (config.has(REGIONS_PER_SERVER) && config.has(REGION_COUNT)) {
            logger.warn("Both {} and {} are set in Titan's configuration, but "
                      + "the former takes precedence and the latter will be ignored.",
                        REGION_COUNT, REGIONS_PER_SERVER);
        }

        /* This static factory calls HBaseConfiguration.addHbaseResources(),
         * which in turn applies the contents of hbase-default.xml and then
         * applies the contents of hbase-site.xml.
         */
        this.hconf = HBaseConfiguration.create();

        // Copy a subset of our commons config into a Hadoop config
        int keysLoaded=0;
        Map<String,Object> configSub = config.getSubset(HBASE_CONFIGURATION_NAMESPACE);
        for (Map.Entry<String,Object> entry : configSub.entrySet()) {
            logger.debug("HBase configuration: setting {}={}", entry.getKey(), entry.getValue());
            if (entry.getValue()==null) continue;
            hconf.set(entry.getKey(), entry.getValue().toString());
            keysLoaded++;
        }

        // Special case for STORAGE_HOSTS
        if (config.has(GraphDatabaseConfiguration.STORAGE_HOSTS)) {
            String zkQuorumKey = "hbase.zookeeper.quorum";
            String csHostList = Joiner.on(",").join(config.get(GraphDatabaseConfiguration.STORAGE_HOSTS));
            hconf.set(zkQuorumKey, csHostList);
            logger.info("Copied host list from {} to {}: {}", GraphDatabaseConfiguration.STORAGE_HOSTS, zkQuorumKey, csHostList);
        }

        logger.debug("HBase configuration: set a total of {} configuration values", keysLoaded);

        this.shortCfNames = config.get(SHORT_CF_NAMES);

        try {
            this.cnx = HConnectionManager.createConnection(hconf);
        } catch (ZooKeeperConnectionException e) {
            throw new PermanentStorageException(e);
        } catch (@SuppressWarnings("hiding") IOException e) { // not thrown in 0.94, but thrown in 0.96+
            throw new PermanentStorageException(e);
        }

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
        IOUtils.closeQuietly(cnx);
    }

    @Override
    public StoreFeatures getFeatures() {

        Configuration c = GraphDatabaseConfiguration.buildConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
                .orderedScan(true).unorderedScan(true).batchMutation(true)
                .multiQuery(true).distributed(true).keyOrdered(true)
//                .timestamps(true)
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
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = convertToCommands(mutations, timestamp.getAdditionTime(times.getUnit()), timestamp.getDeletionTime(times.getUnit()));

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
                table = cnx.getTable(tableName);
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

        sleepAfterWrite(txh, timestamp);
    }

    @Override
    public KeyColumnValueStore openDatabase(final String longName) throws StorageException {

        HBaseKeyColumnValueStore store = openStores.get(longName);

        if (store == null) {

            final String cfName = shortCfNames ? shortenCfName(longName) : longName;

            HBaseKeyColumnValueStore newStore = new HBaseKeyColumnValueStore(this, cnx, tableName, cfName, longName);

            store = openStores.putIfAbsent(longName, newStore); // nothing bad happens if we loose to other thread

            if (store == null ) {
                if (!skipSchemaCheck) {
                    ensureColumnFamilyExists(tableName, cfName);
                }

                store = newStore;
            }
        }

        return store;
    }

    @Override
    public StoreTransaction beginTransaction(final TransactionHandleConfig config) throws StorageException {
        return new HBaseTransaction(config);
    }

    @Override
    public String getName() {
        return tableName;
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

    List<KeyRange> getLocalKeyPartition() {

        List<KeyRange> result = new LinkedList<KeyRange>();

        HTable table = null;
        try {
            table = new HTable(hconf, tableName);

            Map<KeyRange, ServerName> normed =
                    normalizeKeyBounds(table.getRegionLocations());

            for (Map.Entry<KeyRange, ServerName> e : normed.entrySet()) {
                if (NetworkUtil.isLocalConnection(e.getValue().getHostname())) {
                    result.add(e.getKey());
                    logger.debug("Found local key/row partition {} on host {}", e.getKey(), e.getValue());
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

    /**
     * Given a map produced by {@link HTable#getRegionLocations()}, transform
     * each key from an {@link HRegionInfo} to a {@link KeyRange} expressing the
     * region's start and end key bounds using Titan-partitioning-friendly
     * conventions (start inclusive, end exclusive, zero bytes appended where
     * necessary to make all keys at least 4 bytes long).
     * <p/>
     * This method iterates over the entries in its map parameter and performs
     * the following conditional conversions on its keys. "Require" below means
     * either a {@link Preconditions} invocation or an assertion. HRegionInfo
     * sometimes returns start and end keys of zero length; this method replaces
     * zero length keys with null before doing any of the checks described
     * below. The parameter map and the values it contains are only read and
     * never modified.
     *
     * <ul>
     * <li>If an entry's HRegionInfo has null start and end keys, then first
     * require that the parameter map is a singleton, and then return a
     * single-entry map whose {@code KeyRange} has start and end buffers that
     * are both four bytes of zeros.</li>
     * <li>If the entry has a null end key (but non-null start key), put an
     * equivalent entry in the result map with a start key identical to the
     * input, except that zeros are appended to values less than 4 bytes long,
     * and an end key that is four bytes of zeros.
     * <li>If the entry has a null start key (but non-null end key), put an
     * equivalent entry in the result map where the start key is four bytes of
     * zeros, and the end key has zeros appended, if necessary, to make it at
     * least 4 bytes long, after which one is added to the padded value in
     * unsigned 32-bit arithmetic with overflow allowed.</li>
     * <li>Any entry which matches none of the above criteria results in an
     * equivalent entry in the returned map, except that zeros are appended to
     * both keys to make each at least 4 bytes long, and the end key is then
     * incremented as described in the last bullet point.</li>
     * </ul>
     *
     * After iterating over the parameter map, this method checks that it either
     * saw no entries with null keys, one entry with a null start key and a
     * different entry with a null end key, or one entry with both start and end
     * keys null. If any null keys are observed besides these three cases, the
     * method will die with a precondition failure.
     *
     * @param raw
     *            A map of HRegionInfo and ServerName from HBase
     * @return Titan-friendly expression of each region's rowkey boundaries
     */
    private Map<KeyRange, ServerName> normalizeKeyBounds(NavigableMap<HRegionInfo, ServerName> raw) {

        Map.Entry<HRegionInfo, ServerName> nullStart = null;
        Map.Entry<HRegionInfo, ServerName> nullEnd = null;

        ImmutableMap.Builder<KeyRange, ServerName> b = ImmutableMap.builder();

        for (Map.Entry<HRegionInfo, ServerName> e : raw.entrySet()) {
            HRegionInfo regionInfo = e.getKey();
            byte startKey[] = regionInfo.getStartKey();
            byte endKey[]   = regionInfo.getEndKey();

            if (0 == startKey.length) {
                startKey = null;
                logger.trace("Converted zero-length HBase startKey byte array to null");
            }

            if (0 == endKey.length) {
                endKey = null;
                logger.trace("Converted zero-length HBase endKey byte array to null");
            }

            if (null == startKey && null == endKey) {
                Preconditions.checkState(1 == raw.size());
                logger.debug("HBase table {} has a single region {}", tableName, regionInfo);
                // Choose arbitrary shared value = startKey = endKey
                return b.put(new KeyRange(FOUR_ZERO_BYTES, FOUR_ZERO_BYTES), e.getValue()).build();
            } else if (null == startKey) {
                logger.debug("Found HRegionInfo with null startKey on server {}: {}", e.getValue(), regionInfo);
                Preconditions.checkState(null == nullStart);
                nullStart = e;
                // I thought endBuf would be inclusive from the HBase javadoc, but in practice it is exclusive
                StaticBuffer endBuf = StaticArrayBuffer.of(zeroExtend(endKey));
                // Replace null start key with zeroes
                b.put(new KeyRange(FOUR_ZERO_BYTES, endBuf), e.getValue());
            } else if (null == endKey) {
                logger.debug("Found HRegionInfo with null endKey on server {}: {}", e.getValue(), regionInfo);
                Preconditions.checkState(null == nullEnd);
                nullEnd = e;
                // Replace null end key with zeroes
                b.put(new KeyRange(StaticArrayBuffer.of(zeroExtend(startKey)), FOUR_ZERO_BYTES), e.getValue());
            } else {
                Preconditions.checkState(null != startKey);
                Preconditions.checkState(null != endKey);

                // Convert HBase's inclusive end keys into exclusive Titan end keys
                StaticBuffer startBuf = StaticArrayBuffer.of(zeroExtend(startKey));
                StaticBuffer endBuf = StaticArrayBuffer.of(zeroExtend(endKey));

                KeyRange kr = new KeyRange(startBuf, endBuf);
                b.put(kr, e.getValue());
                logger.debug("Found HRegionInfo with non-null end and start keys on server {}: {}", e.getValue(), regionInfo);
            }
        }

        // Require either no null key bounds or a pair of them
        Preconditions.checkState(!(null == nullStart ^ null == nullEnd));

        // Check that every key in the result is at least 4 bytes long
        Map<KeyRange, ServerName> result = b.build();
        for (KeyRange kr : result.keySet()) {
            Preconditions.checkState(4 <= kr.getStart().length());
            Preconditions.checkState(4 <= kr.getEnd().length());
        }

        return result;
    }

    /**
     * If the parameter is shorter than 4 bytes, then create and return a new 4
     * byte array with the input array's bytes followed by zero bytes. Otherwise
     * return the parameter.
     *
     * @param dataToPad non-null but possibly zero-length byte array
     * @return either the parameter or a new array
     */
    private final byte[] zeroExtend(byte[] dataToPad) {
        assert null != dataToPad;

        final int targetLength = 4;

        if (targetLength <= dataToPad.length)
            return dataToPad;

        byte padded[] = new byte[targetLength];

        for (int i = 0; i < dataToPad.length; i++)
            padded[i] = dataToPad[i];

        for (int i = dataToPad.length; i < padded.length; i++)
            padded[i] = (byte)0;

        return padded;
    }

    private String shortenCfName(String longName) throws PermanentStorageException {
        final String s;
        if (SHORT_CF_NAME_MAP.containsKey(longName)) {
            s = SHORT_CF_NAME_MAP.get(longName);
            Preconditions.checkNotNull(s);
            logger.debug("Substituted default CF name \"{}\" with short form \"{}\" to reduce HBase KeyValue size", longName, s);
        } else {
            if (SHORT_CF_NAME_MAP.containsValue(longName)) {
                String fmt = "Must use CF long-form name \"%s\" instead of the short-form name \"%s\" when configured with %s=true";
                String msg = String.format(fmt, SHORT_CF_NAME_MAP.inverse().get(longName), longName, SHORT_CF_NAMES.getName());
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
                desc = createTable(tableName, adm);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }

        return desc;
    }

    private HTableDescriptor createTable(String name, HBaseAdmin adm) throws IOException {
        HTableDescriptor desc = new HTableDescriptor(tableName);

        int count; // total regions to create
        String src;

        if (MIN_REGION_COUNT <= (count = regionCount)) {
            src = "region count configuration";
        } else if (0 < regionsPerServer && MIN_REGION_COUNT <= (count = regionsPerServer * getServerCount(adm))) {
            src = "ClusterStatus server count";
        } else {
            count = -1;
            src = "default";
        }

        if (MIN_REGION_COUNT < count) {
            adm.createTable(desc, getStartKey(count), getEndKey(count), count);
            logger.debug("Created table {} with region count {} from {}", tableName, count, src);
        } else {
            adm.createTable(desc);
            logger.debug("Created table {} with default start key, end key, and region count", tableName);
        }

        return desc;
    }

    /**
     * This method generates the second argument to
     * {@link HBaseAdmin#createTable(HTableDescriptor, byte[], byte[], int)}.
     * <p/>
     * From the {@code createTable} javadoc:
     * "The start key specified will become the end key of the first region of
     * the table, and the end key specified will become the start key of the
     * last region of the table (the first region has a null start key and
     * the last region has a null end key)"
     * <p/>
     * To summarize, the {@code createTable} argument called "startKey" is
     * actually the end key of the first region.
     */
    private byte[] getStartKey(int regionCount) {
        ByteBuffer regionWidth = ByteBuffer.allocate(4);
        regionWidth.putInt((int)(((1L << 32) - 1L) / regionCount)).flip();
        return StaticArrayBuffer.of(regionWidth).getBytes(0, 4);
    }

    /**
     * Companion to {@link #getStartKey(int)}. See its javadoc for details.
     */
    private byte[] getEndKey(int regionCount) {
        ByteBuffer regionWidth = ByteBuffer.allocate(4);
        regionWidth.putInt((int)(((1L << 32) - 1L) / regionCount * (regionCount - 1))).flip();
        return StaticArrayBuffer.of(regionWidth).getBytes(0, 4);
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
                HColumnDescriptor cdesc = new HColumnDescriptor(columnFamily);
                if (null != compression && !compression.equals(COMPRESSION_DEFAULT))
                    HBaseCompatLoader.getCompat().setCompression(cdesc, compression);
                adm.addColumn(tableName, cdesc);

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

        return prov.getTime().getNativeTimestamp();
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

    /**
     * Estimate the number of regionservers in the HBase cluster by calling
     * {@link HBaseAdmin#getClusterStatus()} and then
     * {@link ClusterStatus#getServers()} and finally {@code size()} on the
     * returned server list.
     *
     * @param adm
     *            HBase admin interface
     * @return the number of servers in the cluster or -1 if an error occurred
     */
    private int getServerCount(HBaseAdmin adm) {
        int serverCount = -1;
        try {
            serverCount = adm.getClusterStatus().getServers().size();
            logger.debug("Read {} servers from HBase ClusterStatus", serverCount);
        } catch (IOException e) {
            logger.debug("Unable to retrieve HBase cluster status", e);
        }
        return serverCount;
    }

    private void checkConfigDeprecation(com.thinkaurelius.titan.diskstorage.configuration.Configuration config) {
        if (config.has(GraphDatabaseConfiguration.STORAGE_PORT)) {
            logger.warn("The configuration property {} is ignored for HBase. Set hbase.zookeeper.property.clientPort in hbase-site.xml or {}.hbase.zookeeper.property.clientPort in Titan's configuration file.",
                    GraphDatabaseConfiguration.STORAGE_PORT, HBASE_CONFIGURATION_NAMESPACE);
        }
    }
}

package com.thinkaurelius.titan.diskstorage.hbase;

import static com.thinkaurelius.titan.diskstorage.Backend.EDGESTORE_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.ID_STORE_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.INDEXSTORE_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.LOCK_STORE_SUFFIX;
import static com.thinkaurelius.titan.diskstorage.Backend.SYSTEM_MGMT_LOG_NAME;
import static com.thinkaurelius.titan.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.PermanentBackendException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.TemporaryBackendException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigNamespace;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.CustomizeStoreKCVSManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.time.Timestamps;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.util.system.IOUtils;
import com.thinkaurelius.titan.util.system.NetworkUtil;

/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
@PreInitializeConfigOptions
public class HBaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager, CustomizeStoreKCVSManager {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStoreManager.class);

    public static final ConfigNamespace HBASE_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "hbase", "HBase storage options");

    public static final ConfigOption<Boolean> SHORT_CF_NAMES =
            new ConfigOption<Boolean>(HBASE_NS, "short-cf-names",
            "Whether to shorten the names of Titan's column families to one-character mnemonics " +
            "to conserve storage space", ConfigOption.Type.FIXED, true);

    public static final String COMPRESSION_DEFAULT = "-DEFAULT-";

    public static final ConfigOption<String> COMPRESSION =
            new ConfigOption<String>(HBASE_NS, "compression-algorithm",
            "An HBase Compression.Algorithm enum string which will be applied to newly created column families. " +
            "The compression algorithm must be installed and available on the HBase cluster.  Titan cannot install " +
            "and configure new compression algorithms on the HBase cluster by itself.",
            ConfigOption.Type.MASKABLE, "GZ");

    public static final ConfigOption<Boolean> SKIP_SCHEMA_CHECK =
            new ConfigOption<Boolean>(HBASE_NS, "skip-schema-check",
            "Assume that Titan's HBase table and column families already exist. " +
            "When this is true, Titan will not check for the existence of its table/CFs, " +
            "nor will it attempt to create them under any circumstances.  This is useful " +
            "when running Titan without HBase admin privileges.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<String> HBASE_TABLE =
            new ConfigOption<String>(HBASE_NS, "table",
            "The name of the table Titan will use.  When " + ConfigElement.getPath(SKIP_SCHEMA_CHECK) +
            " is false, Titan will automatically create this table if it does not already exist.",
            ConfigOption.Type.LOCAL, "titan");

    /**
     * Related bug fixed in 0.98.0, 0.94.7, 0.95.0:
     *
     * https://issues.apache.org/jira/browse/HBASE-8170
     */
    public static final int MIN_REGION_COUNT = 3;

    /**
     * The total number of HBase regions to create with Titan's table. This
     * setting only effects table creation; this normally happens just once when
     * Titan connects to an HBase backend for the first time.
     */
    public static final ConfigOption<Integer> REGION_COUNT =
            new ConfigOption<Integer>(HBASE_NS, "region-count",
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
    public static final ConfigOption<Integer> REGIONS_PER_SERVER =
            new ConfigOption<Integer>(HBASE_NS, "regions-per-server",
            "The number of regions per regionserver to set when creating Titan's HBase table",
            ConfigOption.Type.MASKABLE, Integer.class);

    /**
     * If this key is present in either the JVM system properties or the process
     * environment (checked in the listed order, first hit wins), then its value
     * must be the full package and class name of an implementation of
     * {@link HBaseCompat} that has a no-arg public constructor.
     * <p>
     * When this <b>is not</b> set, Titan attempts to automatically detect the
     * HBase runtime version by calling {@link VersionInfo#getVersion()}. Titan
     * then checks the returned version string against a hard-coded list of
     * supported version prefixes and instantiates the associated compat layer
     * if a match is found.
     * <p>
     * When this <b>is</b> set, Titan will not call
     * {@code VersionInfo.getVersion()} or read its hard-coded list of supported
     * version prefixes. Titan will instead attempt to instantiate the class
     * specified (via the no-arg constructor which must exist) and then attempt
     * to cast it to HBaseCompat and use it as such. Titan will assume the
     * supplied implementation is compatible with the runtime HBase version and
     * make no attempt to verify that assumption.
     * <p>
     * Setting this key incorrectly could cause runtime exceptions at best or
     * silent data corruption at worst. This setting is intended for users
     * running exotic HBase implementations that don't support VersionInfo or
     * implementations which return values from {@code VersionInfo.getVersion()}
     * that are inconsistent with Apache's versioning convention. It may also be
     * useful to users who want to run against a new release of HBase that Titan
     * doesn't yet officially support.
     *
     */
    public static final ConfigOption<String> COMPAT_CLASS =
            new ConfigOption<String>(HBASE_NS, "compat-class",
            "The package and class name of the HBaseCompat implementation. HBaseCompat masks version-specific HBase API differences. " +
            "When this option is unset, Titan calls HBase's VersionInfo.getVersion() and loads the matching compat class " +
            "at runtime.  Setting this option forces Titan to instead reflectively load and instantiate the specified class.",
            ConfigOption.Type.MASKABLE, String.class);

    public static final int PORT_DEFAULT = 9160;

    public static final Timestamps PREFERRED_TIMESTAMPS = Timestamps.MILLI;

    public static final ConfigNamespace HBASE_CONFIGURATION_NAMESPACE =
            new ConfigNamespace(HBASE_NS, "ext", "Overrides for hbase-{site,default}.xml options", true);

    private static final BiMap<String, String> SHORT_CF_NAME_MAP =
            ImmutableBiMap.<String, String>builder()
                    .put(INDEXSTORE_NAME, "g")
                    .put(INDEXSTORE_NAME + LOCK_STORE_SUFFIX, "h")
                    .put(ID_STORE_NAME, "i")
                    .put(EDGESTORE_NAME, "e")
                    .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "f")
                    .put(SYSTEM_PROPERTIES_STORE_NAME, "s")
                    .put(SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX, "t")
                    .put(SYSTEM_MGMT_LOG_NAME, "m")
                    .put(SYSTEM_TX_LOG_NAME, "l")
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
    private final String compatClass;
    private final HBaseCompat compat;

    private static final ConcurrentHashMap<HBaseStoreManager, Throwable> openManagers =
            new ConcurrentHashMap<HBaseStoreManager, Throwable>();

    // Mutable instance state
    private final ConcurrentMap<String, HBaseKeyColumnValueStore> openStores;

    public HBaseStoreManager(com.thinkaurelius.titan.diskstorage.configuration.Configuration config) throws BackendException {
        super(config, PORT_DEFAULT);

        checkConfigDeprecation(config);

        this.tableName = config.get(HBASE_TABLE);
        this.compression = config.get(COMPRESSION);
        this.regionCount = config.has(REGION_COUNT) ? config.get(REGION_COUNT) : -1;
        this.regionsPerServer = config.has(REGIONS_PER_SERVER) ? config.get(REGIONS_PER_SERVER) : -1;
        this.skipSchemaCheck = config.get(SKIP_SCHEMA_CHECK);
        this.compatClass = config.has(COMPAT_CLASS) ? config.get(COMPAT_CLASS) : null;
        this.compat = HBaseCompatLoader.getCompat(compatClass);

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
            logger.info("HBase configuration: setting {}={}", entry.getKey(), entry.getValue());
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
            throw new PermanentBackendException(e);
        } catch (@SuppressWarnings("hiding") IOException e) { // not thrown in 0.94, but thrown in 0.96+
            throw new PermanentBackendException(e);
        }

        if (logger.isTraceEnabled()) {
            openManagers.put(this, new Throwable("Manager Opened"));
            dumpOpenManagers();
        }

        logger.debug("Dumping HBase config key=value pairs");
        for (Map.Entry<String, String> entry : hconf) {
            logger.debug("[HBaseConfig] " + entry.getKey() + "=" + entry.getValue());
        }
        logger.debug("End of HBase config key=value pairs");

        openStores = new ConcurrentHashMap<String, HBaseKeyColumnValueStore>();
    }

    @Override
    public Deployment getDeployment() {
        List<KeyRange> local;
        try {
            local = getLocalKeyPartition();
            return null != local && !local.isEmpty() ? Deployment.LOCAL : Deployment.REMOTE;
        } catch (BackendException e) {
            // propagating StorageException might be a better approach
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "hbase[" + tableName + "@" + super.toString() + "]";
    }

    public void dumpOpenManagers() {
        int estimatedSize = openManagers.size();
        logger.trace("---- Begin open HBase store manager list ({} managers) ----", estimatedSize);
        for (HBaseStoreManager m : openManagers.keySet()) {
            logger.trace("Manager {} opened at:", m, openManagers.get(m));
        }
        logger.trace("----   End open HBase store manager list ({} managers)  ----", estimatedSize);
    }

    @Override
    public void close() {
        openStores.clear();
        if (logger.isTraceEnabled())
            openManagers.remove(this);
        IOUtils.closeQuietly(cnx);
    }

    @Override
    public StoreFeatures getFeatures() {

        Configuration c = GraphDatabaseConfiguration.buildConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
                .orderedScan(true).unorderedScan(true).batchMutation(true)
                .multiQuery(true).distributed(true).keyOrdered(true).storeTTL(true)
                .timestamps(true).preferredTimestamps(PREFERRED_TIMESTAMPS)
                .keyConsistent(c);

        try {
            fb.localKeyPartition(getDeployment() == Deployment.LOCAL);
        } catch (Exception e) {
            logger.warn("Unexpected exception during getDeployment()", e);
        }

        return fb.build();
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        final MaskedTimestamp commitTime = new MaskedTimestamp(txh);
        // In case of an addition and deletion with identical timestamps, the
        // deletion tombstone wins.
        // http://hbase.apache.org/book/versions.html#d244e4250
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey =
                convertToCommands(
                        mutations,
                        commitTime.getAdditionTime(times.getUnit()),
                        commitTime.getDeletionTime(times.getUnit()));

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
            throw new TemporaryBackendException(e);
        } catch (InterruptedException e) {
            throw new TemporaryBackendException(e);
        }

        sleepAfterWrite(txh, commitTime);
    }

    @Override
    public KeyColumnValueStore openDatabase(String longName) throws BackendException {
        return openDatabase(longName, -1);
    }

    @Override
    public KeyColumnValueStore openDatabase(final String longName, int ttlInSeconds) throws BackendException {

        HBaseKeyColumnValueStore store = openStores.get(longName);

        if (store == null) {
            final String cfName = shortCfNames ? shortenCfName(longName) : longName;

            HBaseKeyColumnValueStore newStore = new HBaseKeyColumnValueStore(this, cnx, tableName, cfName, longName);

            store = openStores.putIfAbsent(longName, newStore); // nothing bad happens if we loose to other thread

            if (store == null) {
                if (!skipSchemaCheck)
                    ensureColumnFamilyExists(tableName, cfName, ttlInSeconds);

                store = newStore;
            }
        }

        return store;
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
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
    public void clearStorage() throws BackendException {
        HBaseAdmin adm = null;

        try { // first of all, check if table exists, if not - we are done
            adm = getAdminInterface();
            if (!adm.tableExists(tableName)) {
                logger.debug("clearStorage() called before table {} was created, skipping.", tableName);
                return;
            }
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        } finally {
            IOUtils.closeQuietly(adm);
        }

//        long before = System.currentTimeMillis();
//        try {
//            adm.disableTable(tableName);
//            adm.deleteTable(tableName);
//        } catch (IOException e) {
//            throw new PermanentBackendException(e);
//        }
//        ensureTableExists(tableName, getCfNameForStoreName(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME), 0);
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

            long timestamp = times.getTime().getNativeTimestamp();

            try {
                scanner = table.getScanner(scan);

                for (Result res : scanner) {
                    Delete d = new Delete(res.getRow());

                    d.setTimestamp(timestamp);
                    table.delete(d);
                }
            } finally {
                IOUtils.closeQuietly(scanner);
            }
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {

        List<KeyRange> result = new LinkedList<KeyRange>();

        HTable table = null;
        try {
            ensureTableExists(tableName, getCfNameForStoreName(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME), 0);

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
            IOUtils.closeQuietly(table);
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

    private String shortenCfName(String longName) throws PermanentBackendException {
        final String s;
        if (SHORT_CF_NAME_MAP.containsKey(longName)) {
            s = SHORT_CF_NAME_MAP.get(longName);
            Preconditions.checkNotNull(s);
            logger.debug("Substituted default CF name \"{}\" with short form \"{}\" to reduce HBase KeyValue size", longName, s);
        } else {
            if (SHORT_CF_NAME_MAP.containsValue(longName)) {
                String fmt = "Must use CF long-form name \"%s\" instead of the short-form name \"%s\" when configured with %s=true";
                String msg = String.format(fmt, SHORT_CF_NAME_MAP.inverse().get(longName), longName, SHORT_CF_NAMES.getName());
                throw new PermanentBackendException(msg);
            }
            s = longName;
            logger.debug("Kept default CF name \"{}\" because it has no associated short form", s);
        }
        return s;
    }

    private HTableDescriptor ensureTableExists(String tableName, String initialCFName, int ttlInSeconds) throws BackendException {
        HBaseAdmin adm = null;

        HTableDescriptor desc;

        try { // Create our table, if necessary
            adm = getAdminInterface();
            /*
             * Some HBase versions/impls respond badly to attempts to create a
             * table without at least one CF. See #661. Creating a CF along with
             * the table avoids HBase carping.
             */
            if (adm.tableExists(tableName)) {
                desc = adm.getTableDescriptor(tableName.getBytes());
            } else {
                desc = createTable(tableName, initialCFName, ttlInSeconds, adm);
            }
        } catch (IOException e) {
            throw new TemporaryBackendException(e);
        } finally {
            IOUtils.closeQuietly(adm);
        }

        return desc;
    }

    private HTableDescriptor createTable(String tableName, String cfName, int ttlInSeconds, HBaseAdmin adm) throws IOException {
        HTableDescriptor desc = compat.newTableDescriptor(tableName);

        HColumnDescriptor cdesc = new HColumnDescriptor(cfName);
        setCFOptions(cdesc, ttlInSeconds);
        desc.addFamily(cdesc);

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

    private void ensureColumnFamilyExists(String tableName, String columnFamily, int ttlInSeconds) throws BackendException {
        HBaseAdmin adm = null;
        try {
            adm = getAdminInterface();
            HTableDescriptor desc = ensureTableExists(tableName, columnFamily, ttlInSeconds);

            Preconditions.checkNotNull(desc);

            HColumnDescriptor cf = desc.getFamily(columnFamily.getBytes());

            // Create our column family, if necessary
            if (cf == null) {
                try {
                    if (!adm.isTableDisabled(tableName)) {
                        adm.disableTable(tableName);
                    }
                } catch (TableNotEnabledException e) {
                    logger.debug("Table {} already disabled", tableName);
                } catch (IOException e) {
                    throw new TemporaryBackendException(e);
                }

                try {
                    HColumnDescriptor cdesc = new HColumnDescriptor(columnFamily);

                    setCFOptions(cdesc, ttlInSeconds);

                    adm.addColumn(tableName, cdesc);

                    try {
                        logger.debug("Added HBase ColumnFamily {}, waiting for 1 sec. to propogate.", columnFamily);
                        Thread.sleep(1000L);
                    } catch (InterruptedException ie) {
                        throw new TemporaryBackendException(ie);
                    }

                    adm.enableTable(tableName);
                } catch (TableNotFoundException ee) {
                    logger.error("TableNotFoundException", ee);
                    throw new PermanentBackendException(ee);
                } catch (org.apache.hadoop.hbase.TableExistsException ee) {
                    logger.debug("Swallowing exception {}", ee);
                } catch (IOException ee) {
                    throw new TemporaryBackendException(ee);
                }
            }
        } finally {
            IOUtils.closeQuietly(adm);
        }
    }

    private void setCFOptions(HColumnDescriptor cdesc, int ttlInSeconds) {
        if (null != compression && !compression.equals(COMPRESSION_DEFAULT))
            compat.setCompression(cdesc, compression);

        if (ttlInSeconds > 0)
            cdesc.setTimeToLive(ttlInSeconds);
    }

    /**
     * Convert Titan internal Mutation representation into HBase native commands.
     *
     * @param mutations    Mutations to convert into HBase commands.
     * @param putTimestamp The timestamp to use for Put commands.
     * @param delTimestamp The timestamp to use for Delete commands.
     * @return Commands sorted by key converted from Titan internal representation.
     * @throws com.thinkaurelius.titan.diskstorage.PermanentBackendException
     */
    private Map<StaticBuffer, Pair<Put, Delete>> convertToCommands(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
                                                                   final long putTimestamp,
                                                                   final long delTimestamp) throws PermanentBackendException {
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

    private String getCfNameForStoreName(String storeName) throws PermanentBackendException {
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
                    ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_PORT), ConfigElement.getPath(HBASE_CONFIGURATION_NAMESPACE));
        }
    }

    private <T> T runWithAdmin(BackendFunction<HBaseAdmin, T> closure) throws BackendException {
        HBaseAdmin adm = null;
        try {
            adm = new HBaseAdmin(cnx);
            return closure.apply(adm);
        } catch (IOException e) {
            throw new TitanException(e);
        } finally {
            IOUtils.closeQuietly(adm);
        }
    }

    private HBaseAdmin getAdminInterface() {
        try {
            return new HBaseAdmin(cnx);
        } catch (IOException e) {
            throw new TitanException(e);
        }
    }

    /**
     * Similar to {@link Function}, except that the {@code apply} method is allowed
     * to throw {@link BackendException}.
     */
    private static interface BackendFunction<F, T> {

        T apply(F input) throws BackendException;
    }
}

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

package org.janusgraph.diskstorage.hbase;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME;
import static org.janusgraph.diskstorage.Backend.LOCK_STORE_SUFFIX;
import static org.janusgraph.diskstorage.Backend.SYSTEM_MGMT_LOG_NAME;
import static org.janusgraph.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;
import org.janusgraph.util.system.IOUtils;
import org.janusgraph.util.system.NetworkUtil;

/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
@PreInitializeConfigOptions
public class HBaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStoreManager.class);

    public static final ConfigNamespace HBASE_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "hbase", "HBase storage options");

    public static final ConfigOption<Boolean> SHORT_CF_NAMES =
            new ConfigOption<>(HBASE_NS, "short-cf-names",
            "Whether to shorten the names of JanusGraph's column families to one-character mnemonics " +
            "to conserve storage space", ConfigOption.Type.FIXED, true);

    public static final String COMPRESSION_DEFAULT = "-DEFAULT-";

    public static final ConfigOption<String> COMPRESSION =
            new ConfigOption<>(HBASE_NS, "compression-algorithm",
            "An HBase Compression.Algorithm enum string which will be applied to newly created column families. " +
            "The compression algorithm must be installed and available on the HBase cluster.  JanusGraph cannot install " +
            "and configure new compression algorithms on the HBase cluster by itself.",
            ConfigOption.Type.MASKABLE, "GZ");

    public static final ConfigOption<Boolean> SKIP_SCHEMA_CHECK =
            new ConfigOption<>(HBASE_NS, "skip-schema-check",
            "Assume that JanusGraph's HBase table and column families already exist. " +
            "When this is true, JanusGraph will not check for the existence of its table/CFs, " +
            "nor will it attempt to create them under any circumstances.  This is useful " +
            "when running JanusGraph without HBase admin privileges.",
            ConfigOption.Type.MASKABLE, false);

    public static final ConfigOption<String> HBASE_TABLE =
            new ConfigOption<>(HBASE_NS, "table",
            "The name of the table JanusGraph will use.  When " + ConfigElement.getPath(SKIP_SCHEMA_CHECK) +
            " is false, JanusGraph will automatically create this table if it does not already exist.",
            ConfigOption.Type.LOCAL, "janusgraph");

    /**
     * Related bug fixed in 0.98.0, 0.94.7, 0.95.0:
     *
     * https://issues.apache.org/jira/browse/HBASE-8170
     */
    public static final int MIN_REGION_COUNT = 3;

    /**
     * The total number of HBase regions to create with JanusGraph's table. This
     * setting only effects table creation; this normally happens just once when
     * JanusGraph connects to an HBase backend for the first time.
     */
    public static final ConfigOption<Integer> REGION_COUNT =
            new ConfigOption<Integer>(HBASE_NS, "region-count",
            "The number of initial regions set when creating JanusGraph's HBase table",
            ConfigOption.Type.MASKABLE, Integer.class, input -> null != input && MIN_REGION_COUNT <= input);

    /**
     * This setting is used only when {@link #REGION_COUNT} is unset.
     * <p/>
     * If JanusGraph's HBase table does not exist, then it will be created with total
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
            new ConfigOption<>(HBASE_NS, "regions-per-server",
            "The number of regions per regionserver to set when creating JanusGraph's HBase table",
            ConfigOption.Type.MASKABLE, Integer.class);

    /**
     * If this key is present in either the JVM system properties or the process
     * environment (checked in the listed order, first hit wins), then its value
     * must be the full package and class name of an implementation of
     * {@link HBaseCompat} that has a no-arg public constructor.
     * <p>
     * When this <b>is not</b> set, JanusGraph attempts to automatically detect the
     * HBase runtime version by calling {@link VersionInfo#getVersion()}. JanusGraph
     * then checks the returned version string against a hard-coded list of
     * supported version prefixes and instantiates the associated compat layer
     * if a match is found.
     * <p>
     * When this <b>is</b> set, JanusGraph will not call
     * {@code VersionInfo.getVersion()} or read its hard-coded list of supported
     * version prefixes. JanusGraph will instead attempt to instantiate the class
     * specified (via the no-arg constructor which must exist) and then attempt
     * to cast it to HBaseCompat and use it as such. JanusGraph will assume the
     * supplied implementation is compatible with the runtime HBase version and
     * make no attempt to verify that assumption.
     * <p>
     * Setting this key incorrectly could cause runtime exceptions at best or
     * silent data corruption at worst. This setting is intended for users
     * running exotic HBase implementations that don't support VersionInfo or
     * implementations which return values from {@code VersionInfo.getVersion()}
     * that are inconsistent with Apache's versioning convention. It may also be
     * useful to users who want to run against a new release of HBase that JanusGraph
     * doesn't yet officially support.
     *
     */
    public static final ConfigOption<String> COMPAT_CLASS =
            new ConfigOption<>(HBASE_NS, "compat-class",
            "The package and class name of the HBaseCompat implementation. HBaseCompat masks version-specific HBase API differences. " +
            "When this option is unset, JanusGraph calls HBase's VersionInfo.getVersion() and loads the matching compat class " +
            "at runtime.  Setting this option forces JanusGraph to instead reflectively load and instantiate the specified class.",
            ConfigOption.Type.MASKABLE, String.class);

    public static final int PORT_DEFAULT = 9160;

    public static final TimestampProviders PREFERRED_TIMESTAMPS = TimestampProviders.MILLI;

    public static final ConfigNamespace HBASE_CONFIGURATION_NAMESPACE =
            new ConfigNamespace(HBASE_NS, "ext", "Overrides for hbase-{site,default}.xml options", true);

    private static final StaticBuffer FOUR_ZERO_BYTES = BufferUtil.zeroBuffer(4);

    // Immutable instance fields
    private final BiMap<String, String> shortCfNameMap;
    private final String tableName;
    private final String compression;
    private final int regionCount;
    private final int regionsPerServer;
    private final ConnectionMask cnx;
    private final org.apache.hadoop.conf.Configuration hconf;
    private final boolean shortCfNames;
    private final boolean skipSchemaCheck;
    private final String compatClass;
    private final HBaseCompat compat;
    // Cached return value of getDeployment() as requesting it can be expensive.
    private Deployment deployment = null;

    private static final ConcurrentHashMap<HBaseStoreManager, Throwable> openManagers = new ConcurrentHashMap<>();

    // Mutable instance state
    private final ConcurrentMap<String, HBaseKeyColumnValueStore> openStores;

    public HBaseStoreManager(org.janusgraph.diskstorage.configuration.Configuration config) throws BackendException {
        super(config, PORT_DEFAULT);

        shortCfNameMap = createShortCfMap(config);

        Preconditions.checkArgument(null != shortCfNameMap);
        Collection<String> shorts = shortCfNameMap.values();
        Preconditions.checkArgument(Sets.newHashSet(shorts).size() == shorts.size());

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
            logger.warn("Both {} and {} are set in JanusGraph's configuration, but "
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
            //this.cnx = HConnectionManager.createConnection(hconf);
            this.cnx = compat.createConnection(hconf);
        } catch (IOException e) {
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

        openStores = new ConcurrentHashMap<>();
    }

    public static BiMap<String, String> createShortCfMap(Configuration config) {
        return ImmutableBiMap.<String, String>builder()
                .put(INDEXSTORE_NAME, "g")
                .put(INDEXSTORE_NAME + LOCK_STORE_SUFFIX, "h")
                .put(config.get(IDS_STORE_NAME), "i")
                .put(EDGESTORE_NAME, "e")
                .put(EDGESTORE_NAME + LOCK_STORE_SUFFIX, "f")
                .put(SYSTEM_PROPERTIES_STORE_NAME, "s")
                .put(SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX, "t")
                .put(SYSTEM_MGMT_LOG_NAME, "m")
                .put(SYSTEM_TX_LOG_NAME, "l")
                .build();
    }

    @Override
    public Deployment getDeployment() {
        if (null != deployment) {
            return deployment;
        }

        List<KeyRange> local;
        try {
            local = getLocalKeyPartition();
            deployment = null != local && !local.isEmpty() ? Deployment.LOCAL : Deployment.REMOTE;
        } catch (BackendException e) {
            throw new RuntimeException(e);
        }
        return deployment;
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

        Configuration c = GraphDatabaseConfiguration.buildGraphConfiguration();

        StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder()
                .orderedScan(true).unorderedScan(true).batchMutation(true)
                .multiQuery(true).distributed(true).keyOrdered(true).storeTTL(true)
                .timestamps(true).preferredTimestamps(PREFERRED_TIMESTAMPS)
                .optimisticLocking(true).keyConsistent(c);

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
                        commitTime.getAdditionTime(times),
                        commitTime.getDeletionTime(times));

        final List<Row> batch = new ArrayList<>(commandsPerKey.size()); // actual batch operation

        // convert sorted commands into representation required for 'batch' operation
        for (Pair<Put, Delete> commands : commandsPerKey.values()) {
            if (commands.getFirst() != null)
                batch.add(commands.getFirst());

            if (commands.getSecond() != null)
                batch.add(commands.getSecond());
        }

        try {
            TableMask table = null;

            try {
                table = cnx.getTable(tableName);
                table.batch(batch, new Object[batch.size()]);
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
    public KeyColumnValueStore openDatabase(String longName, StoreMetaData.Container metaData) throws BackendException {

        HBaseKeyColumnValueStore store = openStores.get(longName);

        if (store == null) {
            final String cfName = getCfNameForStoreName(longName);

            HBaseKeyColumnValueStore newStore = new HBaseKeyColumnValueStore(this, cnx, tableName, cfName, longName);

            store = openStores.putIfAbsent(longName, newStore); // nothing bad happens if we loose to other thread

            if (store == null) {
                if (!skipSchemaCheck) {
                    int cfTTLInSeconds = -1;
                    if (metaData.contains(StoreMetaData.TTL)) {
                        cfTTLInSeconds = metaData.get(StoreMetaData.TTL);
                    }
                    ensureColumnFamilyExists(tableName, cfName, cfTTLInSeconds);
                }

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
        try (AdminMask adm = getAdminInterface()) {
            adm.clearTable(tableName, times.getTime(times.getTime()));
        } catch (IOException e)
        {
            throw new TemporaryBackendException(e);
        }
//
//
//
//        try { // first of all, check if table exists, if not - we are done
//            adm = getAdminInterface();
//            if (!adm.tableExists(tableName)) {
//                logger.debug("clearStorage() called before table {} was created, skipping.", tableName);
//                return;
//            }
//        } catch (IOException e) {
//            throw new TemporaryBackendException(e);
//        } finally {
//            IOUtils.closeQuietly(adm);
//        }
//
////        long before = System.currentTimeMillis();
////        try {
////            adm.disableTable(tableName);
////            adm.deleteTable(tableName);
////        } catch (IOException e) {
////            throw new PermanentBackendException(e);
////        }
////        ensureTableExists(tableName, getCfNameForStoreName(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME), 0);
////        long after = System.currentTimeMillis();
////        logger.debug("Dropped and recreated table {} in {} ms", tableName, after - before);
//
//        HTable table = null;
//
//        try {
//            table = new HTable(hconf, tableName);
//
//            Scan scan = new Scan();
//            scan.setBatch(100);
//            scan.setCacheBlocks(false);
//            scan.setCaching(2000);
//            scan.setTimeRange(0, Long.MAX_VALUE);
//            scan.setMaxVersions(1);
//
//            ResultScanner scanner = null;
//
//            long timestamp = times.getTime(times.getTime());
//
//            try {
//                scanner = table.getScanner(scan);
//
//                for (Result res : scanner) {
//                    Delete d = new Delete(res.getRow());
//
//                    d.setTimestamp(timestamp);
//                    table.delete(d);
//                }
//            } finally {
//                IOUtils.closeQuietly(scanner);
//            }
//        } catch (IOException e) {
//            throw new TemporaryBackendException(e);
//        } finally {
//            IOUtils.closeQuietly(table);
//        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        List<KeyRange> result = new LinkedList<>();
        try {
            ensureTableExists(
                tableName, getCfNameForStoreName(GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME), 0);
            Map<KeyRange, ServerName> normed = normalizeKeyBounds(cnx.getRegionLocations(tableName));

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
        }
        return result;
    }

    /**
     * Given a map produced by {@link HTable#getRegionLocations()}, transform
     * each key from an {@link HRegionInfo} to a {@link KeyRange} expressing the
     * region's start and end key bounds using JanusGraph-partitioning-friendly
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
     * @param locations A list of HRegionInfo
     * @return JanusGraph-friendly expression of each region's rowkey boundaries
     */
    private Map<KeyRange, ServerName> normalizeKeyBounds(List<HRegionLocation> locations) {

        HRegionLocation nullStart = null;
        HRegionLocation nullEnd = null;

        ImmutableMap.Builder<KeyRange, ServerName> b = ImmutableMap.builder();

        for (HRegionLocation location : locations) {
            HRegionInfo regionInfo = location.getRegionInfo();
            ServerName serverName = location.getServerName();
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
                Preconditions.checkState(1 == locations.size());
                logger.debug("HBase table {} has a single region {}", tableName, regionInfo);
                // Choose arbitrary shared value = startKey = endKey
                return b.put(new KeyRange(FOUR_ZERO_BYTES, FOUR_ZERO_BYTES), serverName).build();
            } else if (null == startKey) {
                logger.debug("Found HRegionInfo with null startKey on server {}: {}", serverName, regionInfo);
                Preconditions.checkState(null == nullStart);
                nullStart = location;
                // I thought endBuf would be inclusive from the HBase javadoc, but in practice it is exclusive
                StaticBuffer endBuf = StaticArrayBuffer.of(zeroExtend(endKey));
                // Replace null start key with zeroes
                b.put(new KeyRange(FOUR_ZERO_BYTES, endBuf), serverName);
            } else if (null == endKey) {
                logger.debug("Found HRegionInfo with null endKey on server {}: {}", serverName, regionInfo);
                Preconditions.checkState(null == nullEnd);
                nullEnd = location;
                // Replace null end key with zeroes
                b.put(new KeyRange(StaticArrayBuffer.of(zeroExtend(startKey)), FOUR_ZERO_BYTES), serverName);
            } else {
                Preconditions.checkState(null != startKey);
                Preconditions.checkState(null != endKey);

                // Convert HBase's inclusive end keys into exclusive JanusGraph end keys
                StaticBuffer startBuf = StaticArrayBuffer.of(zeroExtend(startKey));
                StaticBuffer endBuf = StaticArrayBuffer.of(zeroExtend(endKey));

                KeyRange kr = new KeyRange(startBuf, endBuf);
                b.put(kr, serverName);
                logger.debug("Found HRegionInfo with non-null end and start keys on server {}: {}", serverName, regionInfo);
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

    public static String shortenCfName(BiMap<String, String> shortCfNameMap, String longName) throws PermanentBackendException {
        final String s;
        if (shortCfNameMap.containsKey(longName)) {
            s = shortCfNameMap.get(longName);
            Preconditions.checkNotNull(s);
            logger.debug("Substituted default CF name \"{}\" with short form \"{}\" to reduce HBase KeyValue size", longName, s);
        } else {
            if (shortCfNameMap.containsValue(longName)) {
                String fmt = "Must use CF long-form name \"%s\" instead of the short-form name \"%s\" when configured with %s=true";
                String msg = String.format(fmt, shortCfNameMap.inverse().get(longName), longName, SHORT_CF_NAMES.getName());
                throw new PermanentBackendException(msg);
            }
            s = longName;
            logger.debug("Kept default CF name \"{}\" because it has no associated short form", s);
        }
        return s;
    }

    private HTableDescriptor ensureTableExists(String tableName, String initialCFName, int ttlInSeconds) throws BackendException {
        AdminMask adm = null;

        HTableDescriptor desc;

        try { // Create our table, if necessary
            adm = getAdminInterface();
            /*
             * Some HBase versions/impls respond badly to attempts to create a
             * table without at least one CF. See #661. Creating a CF along with
             * the table avoids HBase carping.
             */
            if (adm.tableExists(tableName)) {
                desc = adm.getTableDescriptor(tableName);
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

    private HTableDescriptor createTable(String tableName, String cfName, int ttlInSeconds, AdminMask adm) throws IOException {
        HTableDescriptor desc = compat.newTableDescriptor(tableName);

        HColumnDescriptor cdesc = new HColumnDescriptor(cfName);
        setCFOptions(cdesc, ttlInSeconds);

        compat.addColumnFamilyToTableDescriptor(desc, cdesc);

        int count; // total regions to create
        String src;

        if (MIN_REGION_COUNT <= (count = regionCount)) {
            src = "region count configuration";
        } else if (0 < regionsPerServer &&
                   MIN_REGION_COUNT <= (count = regionsPerServer * adm.getEstimatedRegionServerCount())) {
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
        AdminMask adm = null;
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
     * Convert JanusGraph internal Mutation representation into HBase native commands.
     *
     * @param mutations    Mutations to convert into HBase commands.
     * @param putTimestamp The timestamp to use for Put commands.
     * @param delTimestamp The timestamp to use for Delete commands.
     * @return Commands sorted by key converted from JanusGraph internal representation.
     * @throws org.janusgraph.diskstorage.PermanentBackendException
     */
    private Map<StaticBuffer, Pair<Put, Delete>> convertToCommands(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
                                                                   final long putTimestamp,
                                                                   final long delTimestamp) throws PermanentBackendException {
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = new HashMap<>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {

            String cfString = getCfNameForStoreName(entry.getKey());
            byte[] cfName = cfString.getBytes();

            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                byte[] key = m.getKey().as(StaticBuffer.ARRAY_FACTORY);
                KCVMutation mutation = m.getValue();

                Pair<Put, Delete> commands = commandsPerKey.get(m.getKey());

                if (commands == null) {
                    commands = new Pair<>();
                    commandsPerKey.put(m.getKey(), commands);
                }

                if (mutation.hasDeletions()) {
                    if (commands.getSecond() == null) {
                        Delete d = new Delete(key);
                        compat.setTimestamp(d, delTimestamp);
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
        return shortCfNames ? shortenCfName(shortCfNameMap, storeName) : storeName;
    }

    private void checkConfigDeprecation(org.janusgraph.diskstorage.configuration.Configuration config) {
        if (config.has(GraphDatabaseConfiguration.STORAGE_PORT)) {
            logger.warn("The configuration property {} is ignored for HBase. Set hbase.zookeeper.property.clientPort in hbase-site.xml or {}.hbase.zookeeper.property.clientPort in JanusGraph's configuration file.",
                    ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_PORT), ConfigElement.getPath(HBASE_CONFIGURATION_NAMESPACE));
        }
    }

    private AdminMask getAdminInterface() {
        try {
            return cnx.getAdmin();
        } catch (IOException e) {
            throw new JanusGraphException(e);
        }
    }
}

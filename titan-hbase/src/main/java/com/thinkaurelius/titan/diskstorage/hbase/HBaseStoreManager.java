package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Storage Manager for HBase
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    private static final Logger logger = LoggerFactory.getLogger(HBaseStoreManager.class);

    public static final String TABLE_NAME_KEY = "tablename";
    public static final String TABLE_NAME_DEFAULT = "titan";

    public static final int PORT_DEFAULT = 9160;

    public static final String HBASE_CONFIGURATION_NAMESPACE = "hbase-config";

    public static final ImmutableMap<String, String> HBASE_CONFIGURATION;

    static {
        HBASE_CONFIGURATION = new ImmutableMap.Builder<String, String>()
                .put(GraphDatabaseConfiguration.HOSTNAME_KEY, "hbase.zookeeper.quorum")
                .put(GraphDatabaseConfiguration.PORT_KEY, "hbase.zookeeper.property.clientPort")
                .build();
    }

    private final String tableName;
    private final org.apache.hadoop.conf.Configuration hconf;

    private final ConcurrentMap<String, HBaseKeyColumnValueStore> openStores;
    private final HTablePool connectionPool;

    private final StoreFeatures features;

    public HBaseStoreManager(org.apache.commons.configuration.Configuration config) throws StorageException {
        super(config, PORT_DEFAULT);

        this.tableName = config.getString(TABLE_NAME_KEY, TABLE_NAME_DEFAULT);

        this.hconf = HBaseConfiguration.create();
        for (Map.Entry<String, String> confEntry : HBASE_CONFIGURATION.entrySet()) {
            if (config.containsKey(confEntry.getKey())) {
                hconf.set(confEntry.getValue(), config.getString(confEntry.getKey()));
            }
        }

        // Copy a subset of our commons config into a Hadoop config
        org.apache.commons.configuration.Configuration hbCommons = config.subset(HBASE_CONFIGURATION_NAMESPACE);

        @SuppressWarnings("unchecked") // I hope commons-config eventually fixes this
                Iterator<String> keys = hbCommons.getKeys();
        int keysLoaded = 0;

        while (keys.hasNext()) {
            String key = keys.next();
            String value = hbCommons.getString(key);
            logger.debug("HBase configuration: setting {}={}", key, value);
            hconf.set(key, value);
            keysLoaded++;
        }

        logger.debug("HBase configuration: set a total of {} configuration values", keysLoaded);

        connectionPool = new HTablePool(hconf, connectionPoolSize);

        openStores = new ConcurrentHashMap<String, HBaseKeyColumnValueStore>();

        // TODO: allowing publicly mutate fields is bad, should be fixed
        features = new StoreFeatures();
        features.supportsOrderedScan = true;
        features.supportsUnorderedScan = true;
        features.supportsBatchMutation = true;
        features.supportsTransactions = false;
        features.supportsConsistentKeyOperations = true;
        features.supportsLocking = false;
        features.isKeyOrdered = false;
        features.isDistributed = true;
        features.hasLocalKeyPartition = false;
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
        return features;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws StorageException {
        final long delTS = System.currentTimeMillis();
        final long putTS = delTS + 1;

        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = convertToCommands(mutations, putTS, delTS);
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

        waitUntil(putTS);
    }

    @Override
    public KeyColumnValueStore openDatabase(String dbName) throws StorageException {
        HBaseKeyColumnValueStore store = openStores.get(dbName);

        if (store == null) {
            HBaseKeyColumnValueStore newStore = new HBaseKeyColumnValueStore(connectionPool, tableName, dbName);

            store = openStores.putIfAbsent(dbName, newStore); // nothing bad happens if we loose to other thread

            if (store == null) { // ensure that CF exists only first time somebody tries to open it
                ensureColumnFamilyExists(tableName, dbName);
                store = newStore;
            }
        }

        return store;
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
    public StoreTransaction beginTransaction(final StoreTxConfig config) throws StorageException {
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

        HTable table = null;

        try {
            table = new HTable(hconf, tableName);

            Scan scan = new Scan();
            scan.setBatch(100);
            scan.setCacheBlocks(false);
            scan.setCaching(2000);

            ResultScanner scanner = null;

            try {
                scanner = table.getScanner(scan);

                for (Result res : scanner) {
                    table.delete(new Delete(res.getRow()));
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

    @Override
    public String getConfigurationProperty(final String key) throws StorageException {
        ensureTableExists(tableName);

        try {
            return getAdminInterface().getTableDescriptor(tableName.getBytes()).getValue(key);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public void setConfigurationProperty(final String key, final String value) throws StorageException {
        byte[] name = tableName.getBytes();

        HTableDescriptor desc = ensureTableExists(tableName);

        try {
            HBaseAdmin adm = getAdminInterface();

            adm.disableTable(tableName);

            desc.setValue(key, value);

            adm.modifyTable(name, desc);
            adm.enableTable(tableName);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }
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
     */
    private static Map<StaticBuffer, Pair<Put, Delete>> convertToCommands(Map<String, Map<StaticBuffer, KCVMutation>> mutations,
                                                                          final long putTimestamp,
                                                                          final long delTimestamp) {
        Map<StaticBuffer, Pair<Put, Delete>> commandsPerKey = new HashMap<StaticBuffer, Pair<Put, Delete>>();

        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            byte[] cfName = entry.getKey().getBytes();

            for (Map.Entry<StaticBuffer, KCVMutation> m : entry.getValue().entrySet()) {
                StaticBuffer key = m.getKey();
                KCVMutation mutation = m.getValue();

                Pair<Put, Delete> commands = commandsPerKey.get(key);

                if (commands == null) {
                    commands = new Pair<Put, Delete>();
                    commandsPerKey.put(key, commands);
                }

                if (mutation.hasDeletions()) {
                    if (commands.getSecond() == null)
                        commands.setSecond(new Delete(key.as(StaticBuffer.ARRAY_FACTORY), delTimestamp, null));

                    for (StaticBuffer b : mutation.getDeletions()) {
                        commands.getSecond().deleteColumns(cfName, b.as(StaticBuffer.ARRAY_FACTORY), delTimestamp);
                    }
                }

                if (mutation.hasAdditions()) {
                    if (commands.getFirst() == null)
                        commands.setFirst(new Put(key.as(StaticBuffer.ARRAY_FACTORY), putTimestamp));

                    for (Entry e : mutation.getAdditions()) {
                        commands.getFirst().add(cfName,
                                e.getArrayColumn(),
                                putTimestamp,
                                e.getArrayValue());
                    }
                }
            }
        }

        return commandsPerKey;
    }

    private static void waitUntil(long until) {
        long now = System.currentTimeMillis();

        while (now <= until) {
            try {
                Thread.sleep(1L);
                now = System.currentTimeMillis();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

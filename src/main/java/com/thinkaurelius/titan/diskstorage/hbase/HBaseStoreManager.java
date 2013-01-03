package com.thinkaurelius.titan.diskstorage.hbase;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.Constants;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Mutation;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.diskstorage.hbase.HBaseKeyColumnValueStore.*;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Experimental storage manager for HBase.
 * 
 * This is not ready for production.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager  {

	private static final Logger log = LoggerFactory.getLogger(HBaseStoreManager.class);
	
    public static final String TABLE_NAME_KEY = "tablename";
    public static final String TABLE_NAME_DEFAULT = "titan";
    public static final String VERSION_PROPERTY_KEY = "last-titan-version";

    public static final int PORT_DEFAULT = 9160;

    public static final String HBASE_CONFIGURATION_NAMESPACE = "hbase-config";

    public static final Map<String,String> HBASE_CONFIGURATION_MAP = new ImmutableMap.Builder<String,String>().
            put(GraphDatabaseConfiguration.HOSTNAME_KEY,"hbase.zookeeper.quorum").
            put(GraphDatabaseConfiguration.PORT_KEY, "hbase.zookeeper.property.clientPort").
            build();

	private final String tableName;
    private final org.apache.hadoop.conf.Configuration hconf;

    private final Map<String,HBaseKeyColumnValueStore> openStores;
    private final StoreFeatures features;
    private final HTablePool connectionPool;
	
    public HBaseStoreManager(org.apache.commons.configuration.Configuration config) throws StorageException {
        super(config,PORT_DEFAULT);
        this.tableName = config.getString(TABLE_NAME_KEY,TABLE_NAME_DEFAULT);
		
		this.hconf = HBaseConfiguration.create();
        for (Map.Entry<String,String> confEntry : HBASE_CONFIGURATION_MAP.entrySet()) {
            if (config.containsKey(confEntry.getKey())) {
                hconf.set(confEntry.getValue(),config.getString(confEntry.getKey()));
            }
        }

        // Copy a subset of our commons config into a Hadoop config
        org.apache.commons.configuration.Configuration hbCommons =
                config.subset(HBASE_CONFIGURATION_NAMESPACE);
        @SuppressWarnings("unchecked") // I hope commons-config eventually fixes this
        Iterator<String> keys = hbCommons.getKeys();
        int keysLoaded = 0;
		
		while (keys.hasNext()) {
			String key = keys.next();
			String value = hbCommons.getString(key);
			log.debug("HBase configuration: setting {}={}", key, value);
			hconf.set(key, value);
			keysLoaded++;
		}
		
		log.debug("HBase configuration: set a total of {} configuration values", keysLoaded);

        connectionPool = new HTablePool(hconf, connectionPoolSize);

        openStores = new HashMap<String, HBaseKeyColumnValueStore>();

        features = new StoreFeatures();
        features.supportsScan=false; features.supportsBatchMutation=true; features.supportsTransactions=false;
        features.supportsConsistentKeyOperations=true; features.supportsLocking=false; features.isKeyOrdered=false;
        features.isDistributed=true; features.hasLocalKeyPartition=false;
    }


    @Override
    public String toString() {
        return "hbase["+tableName+"@"+super.toString()+"]";
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
    public void mutateMany(Map<String, Map<ByteBuffer, Mutation>> mutations, StoreTransaction txh) throws StorageException {

        Map<ByteBuffer, Put> puts = new HashMap<ByteBuffer, Put>();
        Map<ByteBuffer, Delete> dels = new HashMap<ByteBuffer, Delete>();



        final long delTS = System.currentTimeMillis();
        final long putTS = delTS + 1;

        for (Map.Entry<String,Map<ByteBuffer,Mutation>> mutEntry : mutations.entrySet()) {
            byte[] columnFamilyBytes = mutEntry.getKey().getBytes();
            for (Map.Entry<ByteBuffer,Mutation> rowMutation : mutEntry.getValue().entrySet()) {
                ByteBuffer keyBB = rowMutation.getKey();
                Mutation mutation = rowMutation.getValue();

                if (mutation.hasDeletions()) {
                    Delete d = dels.get(keyBB);
                    if (null == d) {
                        d = new Delete(toArray(keyBB), delTS, null);
                        dels.put(keyBB, d);
                    }

                    for (ByteBuffer b : mutation.getDeletions()) {
                        d.deleteColumns(columnFamilyBytes, toArray(b), delTS);
                    }
                }

                if (mutation.hasAdditions()) {
                    Put p = puts.get(keyBB);
                    if (null == p) {
                        p = new Put(toArray(keyBB), putTS);
                        puts.put(keyBB, p);
                    }

                    for (Entry e : mutation.getAdditions()) {
                        byte[] colBytes = toArray(e.getColumn());
                        byte[] valBytes = toArray(e.getValue());
                        p.add(columnFamilyBytes, colBytes, putTS, valBytes);
                    }
                }
            }
        }

        List<Row> batchOps = new LinkedList<Row>();
        for (Delete d : dels.values()) batchOps.add(d);
        for (Put p : puts.values()) batchOps.add(p);
        dels=null; puts=null;

        try {
            HTableInterface table = null;
            try {
                table = connectionPool.getTable(tableName);
                table.batch(batchOps);
                table.flushCommits();
            } finally {
                if (null != table)
                    table.close();
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        } catch (InterruptedException e) {
            throw new TemporaryStorageException(e);
        }

        long now = System.currentTimeMillis();
        while (now <= putTS) {
            try {
                Thread.sleep(1L);
                now = System.currentTimeMillis();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

	@Override
	public KeyColumnValueStore openDatabase(String name) throws StorageException {
        if (openStores.containsKey(name))
            return openStores.get(name);

        HBaseAdmin adm = getAdminInterface();

        HTableDescriptor desc;

        try { // Create our table, if necessary
            if (adm.tableExists(tableName)) {
                desc = adm.getTableDescriptor(tableName.getBytes());
            } else {
                desc = new HTableDescriptor(tableName);
                desc.setValue(VERSION_PROPERTY_KEY, Constants.VERSION);

                adm.createTable(desc);
            }
        } catch (IOException e) {
            throw new TemporaryStorageException(e);
        }

        Preconditions.checkNotNull(desc);

        // Create our column family, if necessary
        if (null == desc.getFamily(name.getBytes())) {
            try {
                adm.disableTable(tableName);
                desc.addFamily(new HColumnDescriptor(name));
                adm.modifyTable(tableName.getBytes(), desc);
                log.debug("Added HBase column family {}", name);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException ie) {
                    throw new TemporaryStorageException(ie);
                }
                adm.enableTable(tableName);
            } catch (TableNotFoundException ee) {
                log.error("TableNotFoundException", ee);
                throw new PermanentStorageException(ee);
            } catch (org.apache.hadoop.hbase.TableExistsException ee) {
                log.debug("Swallowing exception {}", ee);
            } catch (IOException ee) {
                throw new TemporaryStorageException(ee);
            }
        }

        HBaseKeyColumnValueStore store = new HBaseKeyColumnValueStore(connectionPool, tableName, name, this);
        openStores.put(name,store);

        return store;
	}

	@Override
	public StoreTransaction beginTransaction(ConsistencyLevel level) throws StorageException {
		return new HBaseTransaction(level);
	}


    /**
     * Deletes the specified table with all its columns.
     * ATTENTION: Invoking this method will delete the table if it exists and therefore causes data loss.
     *
     */
    @Override
    public void clearStorage() throws StorageException {
		HTable table = null;
		try {
			table = new HTable(hconf, tableName);
		    Scan scan = new Scan();
		    scan.setBatch(100);
		    scan.setCacheBlocks(false);
		    scan.setCaching(2000);
		    ResultScanner resScan = null;
		    try {
		    	resScan = table.getScanner(scan);

		    	for(Result res : resScan) {
					Delete del = new Delete(res.getRow());
					table.delete(del);
			    }
		    } finally {
		    	if(resScan != null) {
		    		resScan.close();
		    	}
		    }
		} catch (IOException e) {
			throw new TemporaryStorageException(e);
		} finally {
			if(table != null) {
				try {
					table.close();
				} catch (IOException e) {
				}
			}
		}
    }

    @Override
    public String getLastSeenTitanVersion() throws StorageException {
        try {
            return getAdminInterface().getTableDescriptor(tableName.getBytes()).getValue(VERSION_PROPERTY_KEY);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }
    }

    @Override
    public void setTitanVersionToLatest() throws StorageException {
        byte[] name = tableName.getBytes();

        try {
            HBaseAdmin adm = getAdminInterface();

            HTableDescriptor desc = adm.getTableDescriptor(name);
            desc.setValue(VERSION_PROPERTY_KEY, Constants.VERSION);

            adm.modifyTable(name, desc);
        } catch (IOException e) {
            throw new PermanentStorageException(e);
        }
    }

    private HBaseAdmin getAdminInterface() {
        try {
            return new HBaseAdmin(hconf);
        } catch (IOException e) {
            throw new TitanException(e);
        }
    }
}

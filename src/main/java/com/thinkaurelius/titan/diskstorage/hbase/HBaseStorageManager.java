package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.idmanagement.OrderedKeyColumnValueIDManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.util.ConfigHelper;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Experimental storage manager for HBase.
 * 
 * This is not ready for production.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class HBaseStorageManager implements KeyColumnValueStoreManager {

	private static final Logger log = LoggerFactory.getLogger(HBaseStorageManager.class);
	
    static final String TABLE_NAME_KEY = "tablename";
    static final String TABLE_NAME_DEFAULT = "titan";
    
    public static final String LOCAL_LOCK_MEDIATOR_PREFIX_DEFAULT = "hbase";

    public static final String HBASE_CONFIGURATION_NAMESPACE = "hbase-config";

    public static final Map<String,String> HBASE_CONFIGURATION_MAP = new ImmutableMap.Builder<String,String>().
            put(GraphDatabaseConfiguration.HOSTNAME_KEY,"hbase.zookeeper.quorum").
            put(GraphDatabaseConfiguration.PORT_KEY, "hbase.zookeeper.property.clientPort").
            build();

	private final String tableName;
    private final OrderedKeyColumnValueIDManager idmanager;
    private final int lockRetryCount;
    
    private final long lockWaitMS, lockExpireMS;
    
    private final byte[] rid;
    
    private final String llmPrefix;
    
    private final org.apache.hadoop.conf.Configuration hconf;
	
    public HBaseStorageManager(org.apache.commons.configuration.Configuration config) throws StorageException {
    	this.rid = ConfigHelper.getRid(config);
    	
        this.tableName = config.getString(TABLE_NAME_KEY,TABLE_NAME_DEFAULT);
		
        this.llmPrefix =
				config.getString(
						ConsistentKeyLockStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY,
						LOCAL_LOCK_MEDIATOR_PREFIX_DEFAULT);
        
		this.lockRetryCount =
				config.getInt(
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
						GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT);
		
		this.lockWaitMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_WAIT_MS,
						GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT);
		
		this.lockExpireMS =
				config.getLong(
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
						GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT);
		


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
		
        idmanager = new OrderedKeyColumnValueIDManager(
        		openDatabase("blocks_allocated", null, null), rid, config);
    }

    @Override
    public StorageFeatures getFeatures() {
        return new StoreFeatures(false,false);
    }

    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        idmanager.setIDBlockSizer(sizer);
    }

    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        return idmanager.getIDBlock(partition);
    }

	@Override
	public KeyColumnValueStore openDatabase(String name)
			throws StorageException {
		
		KeyColumnValueStore lockStore =
				openDatabase(name + "_locks", null, null);
		LocalLockMediator llm = LocalLockMediators.INSTANCE.get(llmPrefix + ":" + name);
		KeyColumnValueStore dataStore =
				openDatabase(name, llm, lockStore);
		
		return dataStore;
	}

	private KeyColumnValueStore openDatabase(String name, LocalLockMediator llm, KeyColumnValueStore lockStore)
			throws StorageException {
		
		HBaseAdmin adm = null;
		try {
			adm = new HBaseAdmin(hconf);
		} catch (IOException e) {
			throw new TitanException(e);
		}
		
		// Create our table, if necessary
		HTableDescriptor desc = null;
		try {
		    desc = new HTableDescriptor(tableName);
			adm.createTable(desc);
		} catch (TableExistsException e) {
			try {
				desc = adm.getTableDescriptor(tableName.getBytes());
			} catch (IOException ee) {
				throw new TemporaryStorageException(ee);
			}
		} catch (IOException e) {
			throw new TemporaryStorageException(e);
		}
		
		assert null != desc;
		
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
			
		assert null != desc;
		
		return new HBaseOrderedKeyColumnValueStore(hconf, tableName, name, lockStore,
				llm, rid, lockRetryCount, lockWaitMS, lockExpireMS);
	}

	@Override
	public StoreTransactionHandle beginTransaction() throws StorageException {
		return new HBaseTransaction();
	}

	@Override
	public void close() {
		//Nothing to do
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
}

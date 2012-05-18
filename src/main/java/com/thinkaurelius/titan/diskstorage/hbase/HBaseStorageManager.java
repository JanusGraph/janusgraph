package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.File;
import java.io.IOException;

import com.thinkaurelius.titan.diskstorage.util.LocalIDManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.GraphDatabaseException;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY;

public class HBaseStorageManager implements StorageManager {

	private static final Logger log = LoggerFactory.getLogger(HBaseStorageManager.class);
	
    private static final String TABLE_NAME_KEY = "tablename";
    private static final String TABLE_NAME_DEFAULT = "titantest";
    

	private final String tableName;
    private final LocalIDManager idmanager;
	
    public HBaseStorageManager(org.apache.commons.configuration.Configuration config) {
        tableName = config.getString(TABLE_NAME_KEY,TABLE_NAME_DEFAULT);
        idmanager = new LocalIDManager(config.getString(STORAGE_DIRECTORY_KEY) + File.separator + LocalIDManager.DEFAULT_NAME);
    }


    @Override
    public long[] getIDBlock(int partition, int blockSize) {
        return idmanager.getIDBlock(partition,blockSize);
    }

	@Override
	public OrderedKeyColumnValueStore openDatabase(String name)
			throws GraphStorageException {
		org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
		HBaseAdmin adm = null;
		try {
			adm = new HBaseAdmin(conf);
		} catch (IOException e) {
			throw new GraphDatabaseException(e);
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
				throw new GraphStorageException(ee);
			}
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
		
		assert null != desc;
		
		// Create our column family, if necessary
		if (null == desc.getFamily(name.getBytes())) {
			try {
				adm.disableTable(tableName);
				desc.addFamily(new HColumnDescriptor(name));
				adm.modifyTable(tableName.getBytes(), desc);
				try {
					Thread.sleep(5000L);
				} catch (InterruptedException ie) {
					throw new RuntimeException(ie);
				}
				adm.enableTable(tableName);
			} catch (TableNotFoundException ee) {
				log.error("TableNotFoundException", ee);
				throw new GraphStorageException(ee);
			} catch (org.apache.hadoop.hbase.TableExistsException ee) {
				log.debug("Swallowing exception {}", ee);
			} catch (IOException ee) {
				throw new GraphStorageException(ee);
			}
		}
			
		assert null != desc;
		
		// Retrieve an object to interact with our now-initialized table
//		HTable table;
//		try {
//			table = new HTable(conf, tableName);
//		} catch (IOException e) {
//			throw new GraphStorageException(e);
//		}
		
		return new HBaseOrderedKeyColumnValueStore(conf, tableName, name);
	}

	@Override
	public TransactionHandle beginTransaction() {
		return new HBaseTransaction();
	}

	@Override
	public void close() {
		//Nothing to do
	}

    public void deleteAll() {
        Configuration conf = HBaseConfiguration.create();
        try {
            HBaseAdmin adm = new HBaseAdmin(conf);
            try {
                adm.disableTable(tableName);
            } catch (Exception e) {
                /*
                     * Swallow exception.  Disabling a table typically throws
                     * an exception because the table doesn't exist or is
                     * already disabled.  If there's a serious problem
                     * interacting with HBase, then the following remove
                     * statement will generate an appropriate exception
                     * (which would propagate up as a RuntimeException).
                     */
            }
            adm.deleteTable(tableName);
        } catch (TableNotFoundException e) {
            // Do nothing
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}

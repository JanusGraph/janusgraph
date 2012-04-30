package com.thinkaurelius.titan.configuration;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStorageManager;

/**
 * Stub HBase configuration.  Stores no actual configuration yet; exists only
 * to provide GraphDatabaseConfiguration with HBase objects at runtime.
 * 
 * @author dalaro
 *
 */
public class HBaseStorageConfiguration extends AbstractStorageConfiguration {

	@Override
	public StorageManager getStorageManager(File directory, boolean readOnly) {
		return new HBaseStorageManager();
	}
	
	@Override
	public OrderedKeyColumnValueStore getEdgeStore(StorageManager manager) {
		return manager.openOrderedDatabase(StorageConfiguration.edgeStoreName);
	}

	@Override
	public OrderedKeyColumnValueStore getPropertyIndex(StorageManager manager) {
		return manager.openOrderedDatabase(StorageConfiguration.propertyIndexName);
	}
	
	public void deleteAll() {
		Configuration conf = HBaseConfiguration.create();
		try {
			HBaseAdmin adm = new HBaseAdmin(conf);
			try {
				adm.disableTable(HBaseStorageManager.HBASE_TABLE_NAME);
			} catch (Exception e) {
				/*
				 * Swallow exception.  Disabling a table typically throws
				 * an exception because the table doesn't exist or is
				 * already disabled.  If there's a serious problem
				 * interacting with HBase, then the following delete
				 * statement will generate an appropriate exception
				 * (which would propagate up as a RuntimeException).
				 */
			}
			adm.deleteTable(HBaseStorageManager.HBASE_TABLE_NAME);
		} catch (TableNotFoundException e) {
			// Do nothing
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void save(org.apache.commons.configuration.Configuration config) {
		// TODO Auto-generated method stub
		
	}

}

package com.thinkaurelius.titan.diskstorage.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.exceptions.GraphDatabaseException;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

public class HBaseStorageManager implements StorageManager {

	private static final Logger log = LoggerFactory.getLogger(HBaseStorageManager.class);
	
	// TODO refactor into a config option
	public static final String HBASE_TABLE_NAME = "titantest";
	
	@Override
	public KeyColumnValueStore openDatabase(String name)
			throws GraphStorageException {
		return openOrderedDatabase(name);
	}

	@Override
	public OrderedKeyColumnValueStore openOrderedDatabase(String name)
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
		    desc = new HTableDescriptor(HBASE_TABLE_NAME);
			adm.createTable(desc);
		} catch (TableExistsException e) {
			try {
				desc = adm.getTableDescriptor(HBASE_TABLE_NAME.getBytes());
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
				adm.disableTable(HBASE_TABLE_NAME);
				desc.addFamily(new HColumnDescriptor(name));
				adm.modifyTable(HBASE_TABLE_NAME.getBytes(), desc);
				try {
					Thread.sleep(5000L);
				} catch (InterruptedException ie) {
					throw new RuntimeException(ie);
				}
				adm.enableTable(HBASE_TABLE_NAME);
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
		HTable table;
		try {
			table = new HTable(conf, HBASE_TABLE_NAME);
		} catch (IOException e) {
			throw new GraphStorageException(e);
		}
		
		return new HBaseOrderedKeyColumnValueStore(table, name);
	}

	@Override
	public TransactionHandle beginTransaction() {
		return new HBaseTransaction();
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

}

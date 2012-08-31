package com.thinkaurelius.titan.diskstorage.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.StorageManager;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class KeyValueStorageManagerAdapter implements StorageManager {

    private final Logger log = LoggerFactory.getLogger(KeyValueStorageManagerAdapter.class);
    
    public static final String KEYLENGTH_NAMESPACE = "keylengths";
    
	private final KeyValueStorageManager manager;
	
	private final ImmutableMap<String,Integer> keyLengths;
	
	public KeyValueStorageManagerAdapter(KeyValueStorageManager manager, Configuration config) {
		this.manager = manager;
		Configuration keylen = config.subset(KEYLENGTH_NAMESPACE);
        ImmutableMap.Builder<String,Integer> builder = ImmutableMap.builder();
        builder.put(GraphDatabaseConfiguration.STORAGE_EDGESTORE_NAME,8);
        Iterator<String> keys = keylen.getKeys();
        while(keys.hasNext()) {
            String name = keys.next();
            int length = keylen.getInt(name);
            Preconditions.checkArgument(length>=0,"Positive keylength expected for database: " + name);
            builder.put(name,Integer.valueOf(length));
        }
        keyLengths = builder.build();
        
	}
		
	@Override
	public TransactionHandle beginTransaction() throws StorageException {
		return manager.beginTransaction();
	}

	@Override
	public void close() throws StorageException {
		manager.close();
	}

    @Override
    public void clearStorage() throws StorageException {
        manager.clearStorage();
    }

    @Override
	public OrderedKeyColumnValueStore openDatabase(String name)
			throws StorageException {
        int keyLength = KeyValueStoreAdapter.variableKeyLength;
        if (keyLengths.containsKey(name)) keyLength = keyLengths.get(name).intValue();
        log.debug("Used key length {} for database {}",keyLength,name);
        return new OrderedKeyValueStoreAdapter(manager.openDatabase(name),keyLength);
	}


    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        manager.setIDBlockSizer(sizer);
    }

    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        return manager.getIDBlock(partition);
    }


}

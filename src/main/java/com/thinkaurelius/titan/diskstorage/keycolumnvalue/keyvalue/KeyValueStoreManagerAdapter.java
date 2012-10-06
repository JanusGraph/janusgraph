package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransactionHandle;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

public class KeyValueStoreManagerAdapter implements KeyColumnValueStoreManager {

    private final Logger log = LoggerFactory.getLogger(KeyValueStoreManagerAdapter.class);
    
    public static final String KEYLENGTH_NAMESPACE = "keylengths";
    
	private final KeyValueStoreManager manager;
	
	private final ImmutableMap<String,Integer> keyLengths;
	
	public KeyValueStoreManagerAdapter(KeyValueStoreManager manager, Configuration config) {
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

    public StoreFeatures getFeatures() {
        return manager.getFeatures();
    }
		
	@Override
	public StoreTransactionHandle beginTransaction() throws StorageException {
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
	public KeyColumnValueStore openDatabase(String name)
			throws StorageException {
        int keyLength = KeyValueStoreAdapter.variableKeyLength;
        if (keyLengths.containsKey(name)) keyLength = keyLengths.get(name).intValue();
        log.debug("Used key length {} for database {}",keyLength,name);
        KeyValueStore store = manager.openDatabase(name);
        return new KeyValueStoreAdapter(store,keyLength);
	}


}

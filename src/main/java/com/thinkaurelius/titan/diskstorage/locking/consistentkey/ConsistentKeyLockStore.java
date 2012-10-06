package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;

import java.nio.ByteBuffer;
import java.util.List;


public class ConsistentKeyLockStore implements KeyColumnValueStore {

    /**
     * Configuration setting key for the lock lock mediator prefix
     */
    public static final String LOCAL_LOCK_MEDIATOR_PREFIX_KEY = "local-lock-mediator-prefix";

    public static final String LOCK_STORE_SUFFIX = ":lock$";


    final KeyColumnValueStore dataStore;

	final KeyColumnValueStore lockStore;
	final LocalLockMediator localLockMediator;
    final ConsistentKeyLockConfiguration configuration;

    public ConsistentKeyLockStore(KeyColumnValueStore dataStore) {
        this.dataStore=dataStore;
        this.lockStore=null;
        this.localLockMediator=null;
        this.configuration=null;
    }

	public ConsistentKeyLockStore(KeyColumnValueStoreManager manager, KeyColumnValueStore dataStore, ConsistentKeyLockConfiguration config) throws StorageException {
		this.dataStore = dataStore;
        this.configuration = config;
        this.localLockMediator = LocalLockMediators.INSTANCE.get(config.localLockMediatorPrefix + ":" + dataStore.getName());
        this.lockStore = manager.openDatabase(dataStore.getName()+LOCK_STORE_SUFFIX);
	}

	public KeyColumnValueStore getDataStore() {
		return dataStore;
	}

	public KeyColumnValueStore getLockStore() {
		return lockStore;
	}

	public LocalLockMediator getLocalLockMediator() {
		return localLockMediator;
	}

	public byte[] getRid() {
		return configuration.rid;
	}

	public int getLockRetryCount() {
		return configuration.lockRetryCount;
	}

	public long getLockExpireMS() {
		return configuration.lockExpireMS;
	}

	public long getLockWaitMS() {
		return configuration.lockWaitMS;
	}
    
    private StoreTransactionHandle getTx(StoreTransactionHandle txh) {
        Preconditions.checkArgument(txh!=null && txh instanceof ConsistentKeyLockTransaction);
        return ((ConsistentKeyLockTransaction)txh).getWrappedTransaction();
    }

    @Override
    public boolean containsKey(ByteBuffer key, StoreTransactionHandle txh) throws StorageException {
        return dataStore.containsKey(key,getTx(txh));
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, int limit, StoreTransactionHandle txh) throws StorageException {
        return dataStore.getSlice(key,columnStart,columnEnd,limit,getTx(txh));
    }

    @Override
    public List<Entry> getSlice(ByteBuffer key, ByteBuffer columnStart, ByteBuffer columnEnd, StoreTransactionHandle txh) throws StorageException {
        return dataStore.getSlice(key,columnStart,columnEnd,getTx(txh));
    }

    @Override
    public ByteBuffer get(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return dataStore.get(key,column,getTx(txh));
    }

    @Override
    public boolean containsKeyColumn(ByteBuffer key, ByteBuffer column, StoreTransactionHandle txh) throws StorageException {
        return dataStore.containsKeyColumn(key,column,getTx(txh));
    }

    @Override
    public void mutate(ByteBuffer key, List<Entry> additions, List<ByteBuffer> deletions, StoreTransactionHandle txh) throws StorageException {
        if (lockStore!=null) {
            ConsistentKeyLockTransaction tx = (ConsistentKeyLockTransaction)txh;
            if (!tx.isMutationStarted()) {
                tx.mutationStarted();
                tx.verifyAllLockClaims();
            }
        }
        dataStore.mutate(key,additions,deletions,getTx(txh));
    }

    @Override
    public void acquireLock(ByteBuffer key, ByteBuffer column, ByteBuffer expectedValue, StoreTransactionHandle txh) throws StorageException {
        if (lockStore!=null) {
            ConsistentKeyLockTransaction tx = (ConsistentKeyLockTransaction)txh;
            if (tx.isMutationStarted()) throw new PermanentLockingException("Attempted to obtain a lock after mutations had been persisted");
            tx.writeBlindLockClaim(this,key,column,expectedValue);
        } else {
            dataStore.acquireLock(key,column,expectedValue,getTx(txh));
        }
    }

    @Override
    public RecordIterator<ByteBuffer> getKeys(StoreTransactionHandle txh) throws StorageException {
        return dataStore.getKeys(getTx(txh));
    }

    @Override
    public String getName() {
        return dataStore.getName();
    }

    @Override
    public void close() throws StorageException {
        dataStore.close();
        if (lockStore!=null) lockStore.close();
    }
}

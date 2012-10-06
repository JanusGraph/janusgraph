package com.thinkaurelius.titan.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class TransactionalIDManager extends AbstractIDManager  {

    private static final Logger log = LoggerFactory.getLogger(TransactionalIDManager.class);

    private static final ByteBuffer DEFAULT_COLUMN = ByteBuffer.allocate(1);

    private final KeyColumnValueStoreManager manager;
    private final KeyColumnValueStore store;

    public TransactionalIDManager(KeyColumnValueStoreManager manager, Configuration config) throws StorageException {
        super(config);
        this.manager = manager;
        this.store = manager.openDatabase(ID_STORE_NAME);
    }

    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        long blockSize = getBlockSize(partition);
        ByteBuffer partitionKey = getPartitionKey(partition);

        for (int retry = 0; retry < idApplicationRetryCount; retry++) {
            StoreTransactionHandle txh = null;
            try {
                txh = manager.beginTransaction(ConsistencyLevel.DEFAULT);
                long nextID = getNextID(partitionKey,blockSize,txh);
                store.mutate(partitionKey, ImmutableList.of(new Entry(DEFAULT_COLUMN, ByteBufferUtil.getLongByteBuffer(nextID))),null,txh);
                txh.commit();
                return new long[]{nextID,nextID+blockSize};
            } catch (StorageException e) {
                log.warn("Storage exception while allocating id block - retrying in {} ms: {}", idApplicationWaitMS,e);
                if (txh!=null) txh.abort();
                if (idApplicationWaitMS>0) TimeUtility.sleepUntil(System.currentTimeMillis() + idApplicationWaitMS, log);
            }
        }
        throw new TemporaryLockingException("Exceeded timeout count ["+ idApplicationRetryCount +"] when attempting to allocate next id block");
    }

    private long getNextID(ByteBuffer partitionKey, long blockSize, StoreTransactionHandle txh) throws StorageException {
        if (!store.containsKeyColumn(partitionKey,DEFAULT_COLUMN,txh)) {
            return BASE_ID;
        } else {
            long latest = store.get(partitionKey,DEFAULT_COLUMN,txh).getLong();
            Preconditions.checkArgument(Long.MAX_VALUE - blockSize > latest, "ID overflow detected");
            return latest+blockSize;
        }
    }

    @Override
    public long peekNextID(int partition) throws StorageException {
        for (int retry = 0; retry < idApplicationRetryCount; retry++) {
            StoreTransactionHandle txh = null;
            try {
                txh = manager.beginTransaction(ConsistencyLevel.DEFAULT);
                long nextID = getNextID(getPartitionKey(partition),getBlockSize(partition),txh);
                txh.commit();
                return nextID;
        } catch (StorageException e) {
                log.warn("Storage exception while reading id block - retrying in {} ms: {}", idApplicationWaitMS,e);
                if (txh!=null) txh.abort();
                if (idApplicationWaitMS>0) TimeUtility.sleepUntil(System.currentTimeMillis() + idApplicationWaitMS, log);
            }
        }
        throw new TemporaryLockingException("Exceeded timeout count ["+ idApplicationRetryCount +"] when attempting to read id block");

    }
}

package com.thinkaurelius.titan.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * {@link com.thinkaurelius.titan.diskstorage.IDAuthority} implementation assuming that the backing store
 * supports consistent transactions.
 * <p/>
 * With transactional isolation guarantees, unique id block assignments are just a matter of reading the current
 * counter, incrementing it and committing the transaction. If the transaction fails, another process applied for the same
 * id and the process is retried.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TransactionalIDManager extends AbstractIDManager {

    private static final Logger log = LoggerFactory.getLogger(TransactionalIDManager.class);

    private static final StaticBuffer DEFAULT_COLUMN = ByteBufferUtil.zeroBuffer(1);

    private final StoreManager manager;
    private final KeyColumnValueStore idStore;

    public TransactionalIDManager(KeyColumnValueStore idStore, StoreManager manager, Configuration config) throws StorageException {
        super(config);
        this.manager = manager;
        this.idStore = idStore;
    }

    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        long blockSize = getBlockSize(partition);
        StaticBuffer partitionKey = getPartitionKey(partition);

        for (int retry = 0; retry < idApplicationRetryCount; retry++) {
            StoreTransaction txh = null;
            try {
                txh = manager.beginTransaction(new StoreTxConfig());
                long current = getCurrentID(partitionKey, txh);
                Preconditions.checkArgument(Long.MAX_VALUE - blockSize > current, "ID overflow detected");
                long next = current + blockSize;
                idStore.mutate(partitionKey, ImmutableList.of(StaticBufferEntry.of(DEFAULT_COLUMN, ByteBufferUtil.getLongBuffer(next))), KeyColumnValueStore.NO_DELETIONS, txh);
                txh.commit();
                return new long[]{current, next};
            } catch (StorageException e) {
                log.warn("Storage exception while allocating id block - retrying in {} ms: {}", idApplicationWaitMS, e);
                if (txh != null) txh.rollback();
                if (idApplicationWaitMS > 0)
                    TimeUtility.INSTANCE.sleepUntil(System.currentTimeMillis() + idApplicationWaitMS, log);
            }
        }
        throw new TemporaryLockingException("Exceeded timeout count [" + idApplicationRetryCount + "] when attempting to allocate next id block");
    }

    @Override
    public StaticBuffer[] getLocalIDPartition() throws StorageException {
        return idStore.getLocalKeyPartition();
    }

    @Override
    public void close() throws StorageException {
        idStore.close();
    }

    private long getCurrentID(StaticBuffer partitionKey, StoreTransaction txh) throws StorageException {
        if (!KCVSUtil.containsKeyColumn(idStore, partitionKey, DEFAULT_COLUMN, txh)) {
            return BASE_ID;
        } else {
            long current = KCVSUtil.get(idStore, partitionKey, DEFAULT_COLUMN, txh).getLong(0);
            return current;
        }
    }
}

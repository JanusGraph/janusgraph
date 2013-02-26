package com.thinkaurelius.titan.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class ConsistentKeyIDManager extends AbstractIDManager {

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyIDManager.class);

    private static final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

    private final StoreManager manager;
    private final KeyColumnValueStore idStore;

    private final int rollbackAttempts = 5;
    private final int rollbackWaitTime = 200;

    public ConsistentKeyIDManager(KeyColumnValueStore idStore, StoreManager manager, Configuration config) throws StorageException {
        super(config);
        Preconditions.checkArgument(manager.getFeatures().supportsConsistentKeyOperations());
        this.manager = manager;
        this.idStore = idStore;
    }

    @Override
    public long peekNextID(int partition) throws StorageException {
        for (int retry = 0; retry < idApplicationRetryCount; retry++) {
            StoreTransaction txh = null;
            try {
                txh = manager.beginTransaction(ConsistencyLevel.KEY_CONSISTENT);
                long nextID = getCurrentID(getPartitionKey(partition), txh);
                txh.commit();
                return nextID;
            } catch (TemporaryStorageException e) {
                log.warn("Temporary storage exception while reading id block - retrying in {} ms: {}", idApplicationWaitMS, e);
                if (txh != null) txh.rollback();
                if (idApplicationWaitMS > 0)
                    TimeUtility.sleepUntil(System.currentTimeMillis() + idApplicationWaitMS, log);
            }
        }
        throw new TemporaryLockingException("Exceeded timeout count [" + idApplicationRetryCount + "] when attempting to read last id block");
    }

    @Override
    public ByteBuffer[] getLocalIDPartition() throws StorageException {
        return idStore.getLocalKeyPartition();
    }

    @Override
    public void close() throws StorageException {
        idStore.close();
    }

    private long getCurrentID(ByteBuffer partitionKey, StoreTransaction txh) throws StorageException {
        List<Entry> blocks = idStore.getSlice(new KeySliceQuery(partitionKey, EMPTY_BUFFER, EMPTY_BUFFER, 5), txh);
        if (blocks == null) throw new TemporaryStorageException("Could not read from storage");

        long latest = BASE_ID;

        for (Entry e : blocks) {
            long counterVal = getBlockValue(e.getColumn());
            if (latest < counterVal) {
                latest = counterVal;
            }
        }
        return latest;
    }


    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        long blockSize = getBlockSize(partition);

        for (int retry = 0; retry < idApplicationRetryCount; retry++) {
            StoreTransaction txh = null;
            try {
                txh = manager.beginTransaction(ConsistencyLevel.KEY_CONSISTENT);
                // Read the latest counter values from the idStore
                ByteBuffer partitionKey = getPartitionKey(partition);
                // calculate the start (inclusive) and end (exclusive) of the allocation we're about to attempt
                long nextStart = getCurrentID(partitionKey, txh);
                Preconditions.checkArgument(Long.MAX_VALUE - blockSize > nextStart, "ID overflow detected");
                long nextEnd = nextStart + blockSize;

                ByteBuffer target = getBlockApplication(nextEnd);


                // attempt to write our claim on the next id block
                boolean success = false;
                try {
                    long before = System.currentTimeMillis();
                    idStore.mutate(partitionKey, Arrays.asList(new Entry(target, EMPTY_BUFFER)), null, txh);
                    long after = System.currentTimeMillis();

                    if (idApplicationWaitMS < after - before) {
                        throw new TemporaryStorageException("Wrote claim for id block [" + nextStart + ", " + nextEnd + ") in " + (after - before) + " ms => too slow, threshold is: " + idApplicationWaitMS);
                    } else {

                        assert 0 != target.remaining();
                        ByteBuffer[] slice = getBlockSlice(nextEnd);

                        /* At this point we've written our claim on [nextStart, nextEnd),
                         * but we haven't yet guaranteed the absence of a contending claim on
                         * the same id block from another machine
                         */

                        TimeUtility.sleepUntil(after + idApplicationWaitMS, log);

                        // Read all id allocation claims on this partition, for the counter value we're claiming
                        List<Entry> blocks = idStore.getSlice(new KeySliceQuery(partitionKey, slice[0], slice[1]), txh);
                        if (blocks == null) throw new TemporaryStorageException("Could not read from storage");
                        if (blocks.isEmpty())
                            throw new PermanentStorageException("It seems there is a race-condition in the block application. " +
                                    "If you have multiple Titan instances running on one physical machine, ensure that they have unique machine idAuthorities");

                        /* If our claim is the lexicographically first one, then our claim
                         * is the most senior one and we own this id block
                         */
                        if (target.equals(blocks.get(0).getColumn())) {

                            long result[] = new long[2];
                            result[0] = nextStart;
                            result[1] = nextEnd;

                            if (log.isDebugEnabled()) {
                                log.debug("Acquired ID block [{},{}) on partition {} (my rid is {})",
                                        new Object[]{nextStart, nextEnd, partition, new String(Hex.encodeHex(rid))});
                            }

                            success = true;
                            return result;
                        } else {
                            // Another claimant beat us to this id block -- try again.
                            log.debug("Failed to acquire ID block [{},{}) (another host claimed it first)", nextStart, nextEnd);
                        }
                    }
                } finally {
                    if (!success) {
                        //Delete claim to not pollute id space
                        try {
                            for (int attempt = 0; attempt < rollbackAttempts; attempt++) {
                                idStore.mutate(partitionKey, null, Arrays.asList(target), txh);
                                break;
                            }
                        } catch (StorageException e) {
                            log.warn("Storage exception while deleting old block application - retrying in {} ms: {}", rollbackWaitTime, e);
                            if (rollbackWaitTime > 0)
                                TimeUtility.sleepUntil(System.currentTimeMillis() + rollbackWaitTime, log);
                        }
                    }
                }
            } catch (TemporaryStorageException e) {
                log.warn("Temporary storage exception while acquiring id block - retrying in {} ms: {}", idApplicationWaitMS, e);
                if (txh != null) txh.rollback();
                txh = null;
                if (idApplicationWaitMS > 0)
                    TimeUtility.sleepUntil(System.currentTimeMillis() + idApplicationWaitMS, log);
            } finally {
                if (txh != null) txh.commit();
            }
        }

        throw new TemporaryLockingException("Exceeded timeout count [" + idApplicationRetryCount + "] when attempting to allocate id block");
    }


    private final ByteBuffer[] getBlockSlice(long blockValue) {
        ByteBuffer[] slice = new ByteBuffer[2];
        slice[0] = ByteBuffer.allocate(16);
        slice[1] = ByteBuffer.allocate(16);
        slice[0].putLong(-blockValue).putLong(0).rewind();
        slice[1].putLong(-blockValue).putLong(-1).rewind();
        return slice;
    }

    private final ByteBuffer getBlockApplication(long blockValue) {
        ByteBuffer bb = ByteBuffer.allocate(
                8 // counter long
                        + 8 // time in ms
                        + rid.length);

        bb.putLong(-blockValue).putLong(System.currentTimeMillis()).put(rid);
        bb.rewind();
        return bb;
    }

    private final long getBlockValue(ByteBuffer column) {
        return -column.getLong();
    }

}

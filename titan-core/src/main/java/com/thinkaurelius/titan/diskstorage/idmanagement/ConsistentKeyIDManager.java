package com.thinkaurelius.titan.diskstorage.idmanagement;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_RANDOMIZE_UNIQUE_ID;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_UNIQUE_ID;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_UNIQUE_ID_BITS;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_USE_LOCAL_CONSISTENCY;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.util.time.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.StandardTransactionHandleConfig;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;

/**
 * {@link com.thinkaurelius.titan.diskstorage.IDAuthority} implementation
 * assuming that the backing store supports consistent key operations.
 * <p/>
 * ID blocks are allocated by first applying for an id block, waiting for a
 * specified period of time and then checking that the application was the first
 * received for that particular id block. If so, the application is considered
 * successful. If not, some other process won the application and a new
 * application is tried.
 * <p/>
 * The partition id is used as the key and since key operations are considered
 * consistent, this protocol guarantees unique id block assignments.
 * <p/>
 * This class uses {@code System#currentTimeMillis()} internally, both for
 * timing writes and for the timestamp values written to the storage backend
 * during the lock application process. It always uses
 * {@code currentTimeMillis()} no matter what
 * {@link GraphDatabaseConfiguration#TIMESTAMP_PROVIDER} has been configured in
 * the enclosing graph.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConsistentKeyIDManager extends AbstractIDManager implements BackendOperation.TransactionalProvider {

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyIDManager.class);

    private static final StaticBuffer LOWER_SLICE = BufferUtil.zeroBuffer(16);
    private static final StaticBuffer UPPER_SLICE = BufferUtil.oneBuffer(16);

    private final StoreManager manager;
    private final KeyColumnValueStore idStore;
    private final StandardTransactionHandleConfig.Builder storeTxConfigBuilder;
    /**
     * This belongs in TitanConfig.
     */
    private final TimestampProvider times;

    private final int rollbackAttempts = 5;
    private final Duration rollbackWaitTime = new StandardDuration(200L, TimeUnit.MILLISECONDS);

    private final int uniqueIdBitWidth;
    private final int uniqueIDUpperBound;
    private final int uniqueId;
    private final boolean randomizeUniqueId;
    //private final ConsistencyLevel consistencLevel;

    private final Random random = new Random();

    public ConsistentKeyIDManager(KeyColumnValueStore idStore, StoreManager manager, Configuration config) throws StorageException {
        super(config);
        Preconditions.checkArgument(manager.getFeatures().isKeyConsistent());
        this.manager = manager;
        this.idStore = idStore;
        this.times = config.get(TIMESTAMP_PROVIDER);
        Preconditions.checkNotNull(times);

        uniqueIdBitWidth = config.get(IDAUTHORITY_UNIQUE_ID_BITS);
        uniqueIDUpperBound = 1<<uniqueIdBitWidth;

        storeTxConfigBuilder = new StandardTransactionHandleConfig.Builder().groupName(metricsPrefix).timestampProvider(times);

        if (config.get(IDAUTHORITY_RANDOMIZE_UNIQUE_ID)) {
            Preconditions.checkArgument(!config.has(IDAUTHORITY_UNIQUE_ID),"Conflicting configuration: a unique id and randomization have been set");
            Preconditions.checkArgument(!config.has(IDAUTHORITY_USE_LOCAL_CONSISTENCY),
                    "Cannot use local consistency with randomization - this leads to data corruption");
            randomizeUniqueId = true;
            uniqueId = -1;
            storeTxConfigBuilder.customOptions(manager.getFeatures().getKeyConsistentTxConfig());
        } else {
            randomizeUniqueId = false;
            if (config.get(IDAUTHORITY_USE_LOCAL_CONSISTENCY)) {
                Preconditions.checkArgument(config.has(IDAUTHORITY_UNIQUE_ID),"Need to configure a unique id in order to use local consistency");
                storeTxConfigBuilder.customOptions(manager.getFeatures().getLocalKeyConsistentTxConfig());
            } else {
                storeTxConfigBuilder.customOptions(manager.getFeatures().getKeyConsistentTxConfig());
            }
            uniqueId = config.get(IDAUTHORITY_UNIQUE_ID);
            Preconditions.checkArgument(uniqueId>=0,"Invalid unique id: %s",uniqueId);
            Preconditions.checkArgument(uniqueId<uniqueIDUpperBound,"Unique id is too large for bit width [%s]: %s",uniqueIdBitWidth,uniqueId);
        }
    }

    @Override
    public List<KeyRange> getLocalIDPartition() throws StorageException {
        return idStore.getLocalKeyPartition();
    }

    @Override
    public void close() throws StorageException {
        idStore.close();
    }

    @Override
    public StoreTransaction openTx() throws StorageException {
        return manager.beginTransaction(storeTxConfigBuilder.build());
    }

    private long getCurrentID(final StaticBuffer partitionKey) throws StorageException {
        List<Entry> blocks = BackendOperation.execute(new BackendOperation.Transactional<List<Entry>>() {
            @Override
            public List<Entry> call(StoreTransaction txh) throws StorageException {
                return idStore.getSlice(new KeySliceQuery(partitionKey, LOWER_SLICE, UPPER_SLICE).setLimit(5), txh);
            }
        },this,times);

        if (blocks == null) throw new TemporaryStorageException("Could not read from storage");
        long latest = BASE_ID;

        for (Entry e : blocks) {
            long counterVal = getBlockValue(e);
            if (latest < counterVal) {
                latest = counterVal;
            }
        }
        return latest;
    }

    private int getUniquePartitionID() {
        int id;
        if (randomizeUniqueId) {
            id = random.nextInt(uniqueIDUpperBound);
        } else id = uniqueId;
        assert id>=0 && id<uniqueIDUpperBound;
        return id;
    }

    protected StaticBuffer getPartitionKey(int partition, int uniqueId) {
        if (uniqueIdBitWidth==0)
            return BufferUtil.getIntBuffer(partition);
        return BufferUtil.getIntBuffer(new int[]{partition, uniqueId});
    }

    @Override
    public synchronized long[] getIDBlock(final int partition, Duration timeout) throws StorageException {
        //partition id can be any integer, even negative, its only a partition identifier

        final Timer methodTime = times.getTimer().start();

        final long blockSize = getBlockSize(partition);
        final long idUpperBound = getIdUpperBound(partition);

        final int bitOffset = (VariableLong.unsignedBitLength(idUpperBound)-1)-uniqueIdBitWidth;
        Preconditions.checkArgument(bitOffset>0,"Unique id bit width [%s] is too wide for partition [%s] id bound [%s]"
                                                ,uniqueIdBitWidth,partition,idUpperBound);
        final long idBlockUpperBound = (1l<<bitOffset);

        final List<String> exhausted = new ArrayList<String>(randomUniqueIDLimit);

        Duration backoffMS = idApplicationWaitMS;

        Preconditions.checkArgument(idBlockUpperBound>blockSize,
                "Block size [%s] is larger than upper bound [%s] for bit width [%s]",blockSize,idBlockUpperBound,uniqueIdBitWidth);

        while (methodTime.elapsed().compareTo(timeout) < 0) {
            final int uniquePID = getUniquePartitionID();
            final StaticBuffer partitionKey = getPartitionKey(partition,uniquePID);
            try {
                long nextStart = getCurrentID(partitionKey);
                if (idBlockUpperBound - blockSize <= nextStart) {
                    log.info("ID overflow detected on partition {} with uniqueid {}. Current id {}, block size {}, and upper bound {} for bit width {}.",
                            partition, uniquePID, nextStart, blockSize, idBlockUpperBound, uniqueIdBitWidth);
                    if (randomizeUniqueId) {
                        exhausted.add(partition + "." + uniquePID);
                        if (exhausted.size() == randomUniqueIDLimit)
                            throw new IDPoolExhaustedException(String.format("Exhausted %d partition.uniqueid pair(s): %s", exhausted.size(), Joiner.on(",").join(exhausted)));
                        else
                            throw new UniqueIDExhaustedException(
                                    String.format("Exhausted ID partition %d with uniqueid %d (uniqueid attempt %d/%d)",
                                            partition, uniquePID, exhausted.size(), randomUniqueIDLimit));
                    }
                    throw new IDPoolExhaustedException("Exhausted id block for partition ["+partition+"] with upper bound: " + idBlockUpperBound);
                }

                // calculate the start (inclusive) and end (exclusive) of the allocation we're about to attempt
                assert idBlockUpperBound - blockSize > nextStart;
                long nextEnd = nextStart + blockSize;
                final StaticBuffer target = getBlockApplication(nextEnd);


                // attempt to write our claim on the next id block
                boolean success = false;
                try {
                    Timer writeTimer = times.getTimer().start();
                    BackendOperation.execute(new BackendOperation.Transactional<Boolean>() {
                        @Override
                        public Boolean call(StoreTransaction txh) throws StorageException {
                            idStore.mutate(partitionKey, Arrays.asList(StaticArrayEntry.of(target)), KeyColumnValueStore.NO_DELETIONS, txh);
                            return true;
                        }
                    },this,times);
                    writeTimer.stop();

                    Duration writeElapsed = writeTimer.elapsed();
                    if (idApplicationWaitMS.compareTo(writeElapsed) < 0) {
                        throw new TemporaryStorageException("Wrote claim for id block [" + nextStart + ", " + nextEnd + ") in " + (writeElapsed) + " => too slow, threshold is: " + idApplicationWaitMS);
                    } else {

                        assert 0 != target.length();
                        final StaticBuffer[] slice = getBlockSlice(nextEnd);

                        /* At this point we've written our claim on [nextStart, nextEnd),
                         * but we haven't yet guaranteed the absence of a contending claim on
                         * the same id block from another machine
                         */

                        sleepAndConvertInterrupts(idApplicationWaitMS);

                        // Read all id allocation claims on this partition, for the counter value we're claiming
                        List<Entry> blocks = BackendOperation.execute(new BackendOperation.Transactional<List<Entry>>() {
                            @Override
                            public List<Entry> call(StoreTransaction txh) throws StorageException {
                                return idStore.getSlice(new KeySliceQuery(partitionKey, slice[0], slice[1]), txh);
                            }
                        },this,times);
                        if (blocks == null) throw new TemporaryStorageException("Could not read from storage");
                        if (blocks.isEmpty())
                            throw new PermanentStorageException("It seems there is a race-condition in the block application. " +
                                    "If you have multiple Titan instances running on one physical machine, ensure that they have unique machine idAuthorities");

                        /* If our claim is the lexicographically first one, then our claim
                         * is the most senior one and we own this id block
                         */
                        if (target.equals(blocks.get(0).getColumnAs(StaticBuffer.STATIC_FACTORY))) {

                            long result[] = new long[2];
                            result[0] = nextStart;
                            result[1] = nextEnd;

                            if (log.isDebugEnabled()) {
                                log.debug("Acquired ID block [{},{}) on partition {} (my rid is {})",
                                        new Object[]{nextStart, nextEnd, partition, new String(uid)});
                            }

                            success = true;
                            //Pad ids
                            for (int i=0;i<result.length;i++) {
                                result[i] = (((long)uniquePID)<<bitOffset) + result[i];
                            }
                            return result;
                        } else {
                            // Another claimant beat us to this id block -- try again.
                            log.debug("Failed to acquire ID block [{},{}) (another host claimed it first)", nextStart, nextEnd);
                        }
                    }
                } finally {
                    if (!success) {
                        //Delete claim to not pollute id space
                        for (int attempt = 0; attempt < rollbackAttempts; attempt++) {
                            try {
                                BackendOperation.execute(new BackendOperation.Transactional<Boolean>() {
                                    @Override
                                    public Boolean call(StoreTransaction txh) throws StorageException {
                                        idStore.mutate(partitionKey, KeyColumnValueStore.NO_ADDITIONS, Arrays.asList(target), txh);
                                        return true;
                                    }
                                }, new BackendOperation.TransactionalProvider() { //Use normal consistency level for these non-critical delete operations
                                    @Override
                                    public StoreTransaction openTx() throws StorageException {
                                        return manager.beginTransaction(storeTxConfigBuilder.build());
                                    }
                                    @Override
                                    public void close() {}
                                },times);

                                break;
                            } catch (StorageException e) {
                                log.warn("Storage exception while deleting old block application - retrying in {}", rollbackWaitTime, e);
                                if (!rollbackWaitTime.isZeroLength())
                                    sleepAndConvertInterrupts(rollbackWaitTime);
                            }
                        }
                    }
                }
            } catch (UniqueIDExhaustedException e) {
                // No need to increment the backoff wait time or to sleep
                log.warn(e.getMessage());
            } catch (TemporaryStorageException e) {
                backoffMS = Durations.min(backoffMS.multiply(2), idApplicationWaitMS.multiply(32));
                log.warn("Temporary storage exception while acquiring id block - retrying in {}: {}", backoffMS, e);
                sleepAndConvertInterrupts(backoffMS);
            }
        }

        throw new TemporaryLockingException(String.format("Reached timeout %d (%s elapsed) when attempting to allocate id block on partition %d",
                timeout, methodTime.toString(), partition));
    }


    private final StaticBuffer[] getBlockSlice(long blockValue) {
        StaticBuffer[] slice = new StaticBuffer[2];
        slice[0] = new WriteByteBuffer(16).putLong(-blockValue).putLong(0).getStaticBuffer();
        slice[1] = new WriteByteBuffer(16).putLong(-blockValue).putLong(-1).getStaticBuffer();
        return slice;
    }

    private final StaticBuffer getBlockApplication(long blockValue) {
        WriteByteBuffer bb = new WriteByteBuffer(
                8 // counter long
                        + 8 // time in ms
                        + uidBytes.length);

        bb.putLong(-blockValue).putLong(System.currentTimeMillis());
        WriteBufferUtil.put(bb, uidBytes);
        return bb.getStaticBuffer();
    }

    private final long getBlockValue(Entry column) {
        return -column.getLong(0);
    }

    private void sleepAndConvertInterrupts(Duration d) throws StorageException {
        try {
            times.sleepPast(times.getTime().add(d));
        } catch (InterruptedException e) {
            throw new PermanentStorageException(e);
        }
    }

    private static class UniqueIDExhaustedException extends StorageException {

        private static final long serialVersionUID = 1L;

        public UniqueIDExhaustedException(String msg) {
            super(msg);
        }

    }
}

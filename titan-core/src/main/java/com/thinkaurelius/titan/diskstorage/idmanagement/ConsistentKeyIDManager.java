package com.thinkaurelius.titan.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.util.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * {@link com.thinkaurelius.titan.diskstorage.IDAuthority} implementation assuming that the backing store
 * supports consistent key operations.
 * <p/>
 * ID blocks are allocated by first applying for an id block, waiting for a specified period of time and then
 * checking that the application was the first received for that particular id block. If so, the application is
 * considered successful. If not, some other process won the application and a new application is tried.
 * <p/>
 * The partition id is used as the key and since key operations are considered consistent, this protocol guarantees
 * unique id block assignments.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConsistentKeyIDManager extends AbstractIDManager implements BackendOperation.TransactionalProvider {

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyIDManager.class);

    private static final StaticBuffer LOWER_SLICE = ByteBufferUtil.zeroBuffer(16);
    private static final StaticBuffer UPPER_SLICE = ByteBufferUtil.oneBuffer(16);

    private final StoreManager manager;
    private final KeyColumnValueStore idStore;

    private final int rollbackAttempts = 5;
    private final int rollbackWaitTime = 200;

    private final int uniqueIdBitWidth;
    private final int uniqueIDUpperBound;
    private final int uniqueId;
    private final boolean randomizeUniqueId;
    private final ConsistencyLevel consistencLevel;

    private final Random random = new Random();

    public ConsistentKeyIDManager(KeyColumnValueStore idStore, StoreManager manager, Configuration config) throws StorageException {
        super(config);
        Preconditions.checkArgument(manager.getFeatures().supportsConsistentKeyOperations());
        this.manager = manager;
        this.idStore = idStore;

        uniqueIdBitWidth = config.getInt(IDAUTHORITY_UNIQUE_ID_BITS_KEY,IDAUTHORITY_UNIQUE_ID_BITS_DEFAULT);
        Preconditions.checkArgument(uniqueIdBitWidth>=0 && uniqueIdBitWidth<=16,"Invalid unique id bit width defined [%s]. Must be in [0,16]",uniqueIdBitWidth);
        uniqueIDUpperBound = 1<<uniqueIdBitWidth;
        if (config.getBoolean(IDAUTHORITY_RANDOMIZE_UNIQUE_ID_KEY,IDAUTHORITY_RANDOMIZE_UNIQUE_ID_DEFAULT)) {
            Preconditions.checkArgument(!config.containsKey(IDAUTHORITY_UNIQUE_ID_KEY),"Conflicting configuration: a unique id and randomization have been set");
            Preconditions.checkArgument(!config.getBoolean(IDAUTHORITY_USE_LOCAL_CONSISTENCY_KEY, IDAUTHORITY_USE_LOCAL_CONSISTENCY_DEFAULT),
                    "Cannot use local consistency with randomization - this leads to data corruption");
            randomizeUniqueId = true;
            uniqueId = -1;
            consistencLevel = ConsistencyLevel.KEY_CONSISTENT;
        } else {
            randomizeUniqueId = false;
            if (config.getBoolean(IDAUTHORITY_USE_LOCAL_CONSISTENCY_KEY,IDAUTHORITY_USE_LOCAL_CONSISTENCY_DEFAULT)) {
                Preconditions.checkArgument(config.containsKey(IDAUTHORITY_UNIQUE_ID_KEY),"Need to configure a unique id in order to use local consistency");
                consistencLevel = ConsistencyLevel.LOCAL_KEY_CONSISTENT;
            } else {
                consistencLevel = ConsistencyLevel.KEY_CONSISTENT;
            }
            uniqueId = config.getInt(IDAUTHORITY_UNIQUE_ID_KEY,IDAUTHORITY_UNIQUE_ID_DEFAULT);
            Preconditions.checkArgument(uniqueId>=0,"Invalid unique id: %s",uniqueId);
            Preconditions.checkArgument(uniqueId<uniqueIDUpperBound,"Unique id is too large for bit width [%s]: %s",uniqueIdBitWidth,uniqueId);
        }
    }

    @Override
    public StaticBuffer[] getLocalIDPartition() throws StorageException {
        return idStore.getLocalKeyPartition();
    }

    @Override
    public void close() throws StorageException {
        idStore.close();
    }

    @Override
    public StoreTransaction openTx() throws StorageException {
        return manager.beginTransaction(new StoreTxConfig(consistencLevel, metricsPrefix));
    }

    private long getCurrentID(final StaticBuffer partitionKey) throws StorageException {
        List<Entry> blocks = BackendOperation.execute(new BackendOperation.Transactional<List<Entry>>() {
            @Override
            public List<Entry> call(StoreTransaction txh) throws StorageException {
                return idStore.getSlice(new KeySliceQuery(partitionKey, LOWER_SLICE, UPPER_SLICE).setLimit(5), txh);
            }
        },this);

        if (blocks == null) throw new TemporaryStorageException("Could not read from storage");
        long latest = BASE_ID;

        for (Entry e : blocks) {
            long counterVal = getBlockValue(e.getReadColumn());
            if (latest < counterVal) {
                latest = counterVal;
            }
        }
        return latest;
    }

    private int getUniqueID() {
        int id;
        if (randomizeUniqueId) {
            id = random.nextInt(uniqueIDUpperBound);
        } else id = uniqueId;
        assert id>=0 && id<uniqueIDUpperBound;
        return id;
    }

    protected StaticBuffer getPartitionKey(int partition, int uniqueId) {
        if (uniqueIdBitWidth==0)
            return ByteBufferUtil.getIntBuffer(partition);
        return ByteBufferUtil.getIntBuffer(new int[]{partition,uniqueId});
    }

    @Override
    public long[] getIDBlock(int partition) throws StorageException {
        //partition id can be any integer, even negative, its only a partition identifier

        final long blockSize = getBlockSize(partition);
        final long idUpperBound = getIdUpperBound(partition);

        final int bitOffset = (VariableLong.unsignedBitLength(idUpperBound)-1)-uniqueIdBitWidth;
        Preconditions.checkArgument(bitOffset>0,"Unique id bit width [%s] is too wide for partition [%s] id bound [%s]"
                                                ,uniqueIdBitWidth,partition,idUpperBound);
        final long idBlockUpperBound = (1l<<bitOffset);


        Preconditions.checkArgument(idBlockUpperBound>blockSize,
                "Block size [%s] is larger than upper bound [%s] for bit width [%s]",blockSize,idBlockUpperBound,uniqueIdBitWidth);

        for (int retry = 0; retry < idApplicationRetryCount; retry++) {
            final int uniqueID = getUniqueID();
            try {
                // Read the latest counter values from the idStore
                final StaticBuffer partitionKey = getPartitionKey(partition,uniqueID);
                // calculate the start (inclusive) and end (exclusive) of the allocation we're about to attempt
                long nextStart = getCurrentID(partitionKey);
                if (idBlockUpperBound - blockSize <= nextStart) {
                    log.info("ID overflow detected. Current id {}, block size {} and upper bound {} for bit width {}",
                            nextStart,blockSize,idBlockUpperBound,uniqueIdBitWidth);
                    if (randomizeUniqueId && (retry+1)<idApplicationRetryCount) {
                        continue;
                    }
                    throw new IDPoolExhaustedException("Exhausted id block for partition ["+partition+"] with upper bound: " + idBlockUpperBound);
                }

                assert idBlockUpperBound - blockSize > nextStart;
                long nextEnd = nextStart + blockSize;
                final StaticBuffer target = getBlockApplication(nextEnd);


                // attempt to write our claim on the next id block
                boolean success = false;
                try {
                    long before = System.currentTimeMillis();
                    BackendOperation.execute(new BackendOperation.Transactional<Boolean>() {
                        @Override
                        public Boolean call(StoreTransaction txh) throws StorageException {
                            idStore.mutate(partitionKey, Arrays.asList(StaticBufferEntry.of(target, ByteBufferUtil.emptyBuffer())), KeyColumnValueStore.NO_DELETIONS, txh);
                            return true;
                        }
                    },this);
                    long after = System.currentTimeMillis();

                    if (idApplicationWaitMS < after - before) {
                        throw new TemporaryStorageException("Wrote claim for id block [" + nextStart + ", " + nextEnd + ") in " + (after - before) + " ms => too slow, threshold is: " + idApplicationWaitMS);
                    } else {

                        assert 0 != target.length();
                        final StaticBuffer[] slice = getBlockSlice(nextEnd);

                        /* At this point we've written our claim on [nextStart, nextEnd),
                         * but we haven't yet guaranteed the absence of a contending claim on
                         * the same id block from another machine
                         */

                        TimeUtility.INSTANCE.sleepUntil(after + idApplicationWaitMS, log);

                        // Read all id allocation claims on this partition, for the counter value we're claiming
                        List<Entry> blocks = BackendOperation.execute(new BackendOperation.Transactional<List<Entry>>() {
                            @Override
                            public List<Entry> call(StoreTransaction txh) throws StorageException {
                                return idStore.getSlice(new KeySliceQuery(partitionKey, slice[0], slice[1]), txh);
                            }
                        },this);
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
                            //Pad ids
                            for (int i=0;i<result.length;i++) {
                                result[i] = (((long)uniqueID)<<bitOffset) + result[i];
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
                                        return manager.beginTransaction(new StoreTxConfig(ConsistencyLevel.DEFAULT,metricsPrefix));
                                    }
                                });

                                break;
                            } catch (StorageException e) {
                                log.warn("Storage exception while deleting old block application - retrying in {} ms", rollbackWaitTime, e);
                                if (rollbackWaitTime > 0)
                                    TimeUtility.INSTANCE.sleepUntil(System.currentTimeMillis() + rollbackWaitTime, log);
                            }
                        }
                    }
                }
            } catch (TemporaryStorageException e) {
                log.warn("Temporary storage exception while acquiring id block - retrying in {} ms: {}", idApplicationWaitMS, e);
                if (idApplicationWaitMS > 0)
                    TimeUtility.INSTANCE.sleepUntil(System.currentTimeMillis() + idApplicationWaitMS, log);
            }
        }

        throw new TemporaryLockingException("Exceeded timeout count [" + idApplicationRetryCount + "] when attempting to allocate id block");
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
                        + rid.length);

        bb.putLong(-blockValue).putLong(System.currentTimeMillis());
        WriteBufferUtil.put(bb, rid);
        return bb.getStaticBuffer();
    }

    private final long getBlockValue(ReadBuffer column) {
        return -column.getLong();
    }

}

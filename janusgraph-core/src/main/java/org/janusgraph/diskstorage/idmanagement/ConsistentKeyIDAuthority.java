// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.TemporaryLockingException;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.WriteBufferUtil;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.diskstorage.util.time.Durations;
import org.janusgraph.diskstorage.util.time.Timer;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.util.StringUtils;
import org.janusgraph.util.stats.NumberUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_CAV_BITS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_CAV_RETRIES;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_CAV_TAG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDAUTHORITY_CONFLICT_AVOIDANCE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * {@link org.janusgraph.diskstorage.IDAuthority} implementation
 * assuming that the backing store supports consistent key operations.
 * <p>
 * ID blocks are allocated by first applying for an id block, waiting for a
 * specified period of time and then checking that the application was the first
 * received for that particular id block. If so, the application is considered
 * successful. If not, some other process won the application and a new
 * application is tried.
 * <p>
 * The partition id is used as the key and since key operations are considered
 * consistent, this protocol guarantees unique id block assignments.
 * <p>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConsistentKeyIDAuthority extends AbstractIDAuthority implements BackendOperation.TransactionalProvider {

    private static final Logger log = LoggerFactory.getLogger(ConsistentKeyIDAuthority.class);

    /*
     * ID columns are 17 or more bytes long:
     *
     * -----------------------------------------------------------
     * | 8 bytes counter | 8 bytes timestamp | var bytes rid/uid |
     * -----------------------------------------------------------
     *
     * The argument for the following two slice bounds mirrors the
     * argument for choosing bounds in ConsistentKeyLocker.
     */
    private static final StaticBuffer LOWER_SLICE = BufferUtil.zeroBuffer(1);
    private static final StaticBuffer UPPER_SLICE = BufferUtil.oneBuffer(17);
    private static final int ROLLBACK_ATTEMPTS = 5;

    private final StoreManager manager;
    private final KeyColumnValueStore idStore;
    private final StandardBaseTransactionConfig.Builder storeTxConfigBuilder;
    /**
     * This belongs in JanusGraphConfig.
     */
    private final TimestampProvider times;

    private final Duration rollbackWaitTime = Duration.ofMillis(200L);

    private final int partitionBitWidth;

    private final int uniqueIdBitWidth;
    private final int uniqueIDUpperBound;
    private final int uniqueId;
    private final boolean randomizeUniqueId;
    protected final int randomUniqueIDLimit;
    private final Duration waitGracePeriod;
    private final boolean supportsInterruption;

    private final Random random = new Random();

    public ConsistentKeyIDAuthority(KeyColumnValueStore idStore, StoreManager manager, Configuration config) throws BackendException {
        super(config);
        Preconditions.checkArgument(manager.getFeatures().isKeyConsistent());
        this.manager = manager;
        this.idStore = idStore;
        this.times = config.get(TIMESTAMP_PROVIDER);
        this.waitGracePeriod = idApplicationWaitMS.dividedBy(10);
        Preconditions.checkNotNull(times);

        supportsInterruption = manager.getFeatures().supportsInterruption();

        partitionBitWidth = NumberUtil.getPowerOf2(config.get(CLUSTER_MAX_PARTITIONS));
        Preconditions.checkArgument(partitionBitWidth >=0 && partitionBitWidth <=16);

        uniqueIdBitWidth = config.get(IDAUTHORITY_CAV_BITS);
        Preconditions.checkArgument(uniqueIdBitWidth<=16 && uniqueIdBitWidth>=0);
        uniqueIDUpperBound = 1<<uniqueIdBitWidth;

        storeTxConfigBuilder = new StandardBaseTransactionConfig.Builder().groupName(metricsPrefix).timestampProvider(times);

        final ConflictAvoidanceMode conflictAvoidanceMode = config.get(IDAUTHORITY_CONFLICT_AVOIDANCE);

        if (conflictAvoidanceMode.equals(ConflictAvoidanceMode.GLOBAL_AUTO)) {
            Preconditions.checkArgument(!config.has(IDAUTHORITY_CAV_TAG),"Conflicting configuration: a unique id and randomization have been set");
            randomizeUniqueId = true;
            randomUniqueIDLimit = config.get(IDAUTHORITY_CAV_RETRIES);
            Preconditions.checkArgument(randomUniqueIDLimit<uniqueIDUpperBound,"Cannot have more uid retries [%d] than available values [%d]",
                    randomUniqueIDLimit,uniqueIDUpperBound);
            uniqueId = -1;
            storeTxConfigBuilder.customOptions(manager.getFeatures().getKeyConsistentTxConfig());
        } else {
            randomizeUniqueId = false;
            Preconditions.checkArgument(!config.has(IDAUTHORITY_CAV_RETRIES),"Retry count is only meaningful when " + IDAUTHORITY_CONFLICT_AVOIDANCE + " is set to " + ConflictAvoidanceMode.GLOBAL_AUTO);
            randomUniqueIDLimit = 0;
            if (conflictAvoidanceMode.equals(ConflictAvoidanceMode.LOCAL_MANUAL)) {
                Preconditions.checkArgument(config.has(IDAUTHORITY_CAV_TAG),"Need to configure a unique id in order to use local consistency");
                storeTxConfigBuilder.customOptions(manager.getFeatures().getLocalKeyConsistentTxConfig());
            } else {
                storeTxConfigBuilder.customOptions(manager.getFeatures().getKeyConsistentTxConfig());
            }
            uniqueId = config.get(IDAUTHORITY_CAV_TAG);
            Preconditions.checkArgument(uniqueId>=0,"Invalid unique id: %s",uniqueId);
            Preconditions.checkArgument(uniqueId<uniqueIDUpperBound,"Unique id is too large for bit width [%s]: %s",uniqueIdBitWidth,uniqueId);
        }
        Preconditions.checkArgument(randomUniqueIDLimit>=0);
    }

    @Override
    public List<KeyRange> getLocalIDPartition() throws BackendException {
        return manager.getLocalKeyPartition();
    }

    @Override
    public void close() throws BackendException {
        idStore.close();
    }

    @Override
    public boolean supportsInterruption() {
        return supportsInterruption;
    }

    @Override
    public StoreTransaction openTx() throws BackendException {
        return manager.beginTransaction(storeTxConfigBuilder.build());
    }

    private long getCurrentID(final StaticBuffer partitionKey) throws BackendException {
        final List<Entry> blocks = BackendOperation.execute(
            (BackendOperation.Transactional<List<Entry>>) txh -> idStore.getSlice(new KeySliceQuery(partitionKey, LOWER_SLICE, UPPER_SLICE).setLimit(5), txh),this,times);

        if (blocks == null) throw new TemporaryBackendException("Could not read from storage");
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

    private StaticBuffer getPartitionKey(int partition, int idNamespace, int uniqueId) {
        assert partition>=0 && partition<(1<< partitionBitWidth);
        assert idNamespace>=0;
        assert uniqueId>=0 && uniqueId<(1<<uniqueIdBitWidth);

        int[] components = new int[2];
        components[0] = (partitionBitWidth >0?(partition<<(Integer.SIZE- partitionBitWidth)):0) + uniqueId;
        components[1]=idNamespace;
        return BufferUtil.getIntBuffer(components);
    }

    @Override
    public synchronized IDBlock getIDBlock(final int partition, final int idNamespace, Duration timeout) throws BackendException {
        Preconditions.checkArgument(partition>=0 && partition<(1<< partitionBitWidth),"Invalid partition id [%s] for bit width [%s]",partition, partitionBitWidth);
        Preconditions.checkArgument(idNamespace>=0); //can be any non-negative value

        final Timer methodTime = times.getTimer().start();

        final long blockSize = getBlockSize(idNamespace);
        final long idUpperBound = getIdUpperBound(idNamespace);

        final int maxAvailableBits = (VariableLong.unsignedBitLength(idUpperBound)-1)-uniqueIdBitWidth;
        Preconditions.checkArgument(maxAvailableBits>0,"Unique id bit width [%s] is too wide for id-namespace [%s] id bound [%s]"
                                                ,uniqueIdBitWidth,idNamespace,idUpperBound);
        final long idBlockUpperBound = (1L <<maxAvailableBits);

        final List<Integer> exhaustedUniquePIDs = new ArrayList<>(randomUniqueIDLimit);

        Duration backoffMS = idApplicationWaitMS;

        Preconditions.checkArgument(idBlockUpperBound>blockSize,
                "Block size [%s] is larger than upper bound [%s] for bit width [%s]",blockSize,idBlockUpperBound,uniqueIdBitWidth);

        while (methodTime.elapsed().compareTo(timeout) < 0) {
            final int uniquePID = getUniquePartitionID();
            final StaticBuffer partitionKey = getPartitionKey(partition,idNamespace,uniquePID);
            try {
                long nextStart = getCurrentID(partitionKey);
                if (idBlockUpperBound - blockSize <= nextStart) {
                    log.info("ID overflow detected on partition({})-namespace({}) with uniqueid {}. Current id {}, block size {}, and upper bound {} for bit width {}.",
                            partition, idNamespace, uniquePID, nextStart, blockSize, idBlockUpperBound, uniqueIdBitWidth);
                    if (randomizeUniqueId) {
                        exhaustedUniquePIDs.add(uniquePID);
                        if (exhaustedUniquePIDs.size() == randomUniqueIDLimit)
                            throw new IDPoolExhaustedException(String.format("Exhausted %d uniqueid(s) on partition(%d)-namespace(%d): %s",
                                exhaustedUniquePIDs.size(), partition, idNamespace,
                                StringUtils.join(exhaustedUniquePIDs, ",")));
                        else
                            throw new UniqueIDExhaustedException(
                                    String.format("Exhausted ID partition(%d)-namespace(%d) with uniqueid %d (uniqueid attempt %d/%d)",
                                            partition, idNamespace, uniquePID, exhaustedUniquePIDs.size(), randomUniqueIDLimit));
                    }
                    throw new IDPoolExhaustedException("Exhausted id block for partition("+partition+")-namespace("+idNamespace+") with upper bound: " + idBlockUpperBound);
                }

                // calculate the start (inclusive) and end (exclusive) of the allocation we're about to attempt
                assert idBlockUpperBound - blockSize > nextStart;
                long nextEnd = nextStart + blockSize;
                StaticBuffer target = null;

                // attempt to write our claim on the next id block
                boolean success = false;
                try {
                    Timer writeTimer = times.getTimer().start();
                    target = getBlockApplication(nextEnd, writeTimer.getStartTime());
                    final StaticBuffer finalTarget = target; // copy for the inner class
                    BackendOperation.execute(txh -> {
                        idStore.mutate(partitionKey, Collections.singletonList(StaticArrayEntry.of(finalTarget)), KeyColumnValueStore.NO_DELETIONS, txh);
                        return true;
                    },this,times);
                    writeTimer.stop();

                    final boolean distributed = manager.getFeatures().isDistributed();
                    Duration writeElapsed = writeTimer.elapsed();
                    if (idApplicationWaitMS.compareTo(writeElapsed) < 0 && distributed) {
                        throw new TemporaryBackendException("Wrote claim for id block [" + nextStart + ", " + nextEnd + ") in " + (writeElapsed) + " => too slow, threshold is: " + idApplicationWaitMS);
                    } else {

                        assert 0 != target.length();
                        final StaticBuffer[] slice = getBlockSlice(nextEnd);

                        /* At this point we've written our claim on [nextStart, nextEnd),
                         * but we haven't yet guaranteed the absence of a contending claim on
                         * the same id block from another machine
                         */

                        if (distributed) {
                            sleepAndConvertInterrupts(idApplicationWaitMS.plus(waitGracePeriod));
                        }

                        // Read all id allocation claims on this partition, for the counter value we're claiming
                        final List<Entry> blocks = BackendOperation.execute(
                            (BackendOperation.Transactional<List<Entry>>) txh -> idStore.getSlice(new KeySliceQuery(partitionKey, slice[0], slice[1]), txh),this,times);
                        if (blocks == null) throw new TemporaryBackendException("Could not read from storage");
                        if (blocks.isEmpty())
                            throw new PermanentBackendException("It seems there is a race-condition in the block application. " +
                                    "If you have multiple JanusGraph instances running on one physical machine, ensure that they have unique machine idAuthorities");

                        /* If our claim is the lexicographically first one, then our claim
                         * is the most senior one and we own this id block
                         */
                        if (target.equals(blocks.get(0).getColumnAs(StaticBuffer.STATIC_FACTORY))) {

                            ConsistentKeyIDBlock idBlock = new ConsistentKeyIDBlock(nextStart,blockSize,uniqueIdBitWidth,uniquePID);

                            if (log.isDebugEnabled()) {
                                log.debug("Acquired ID block [{}] on partition({})-namespace({}) (my rid is {})",
                                    idBlock, partition, idNamespace, uid);
                            }

                            success = true;
                            return idBlock;
                        } else {
                            // Another claimant beat us to this id block -- try again.
                            log.debug("Failed to acquire ID block [{},{}) (another host claimed it first)", nextStart, nextEnd);
                        }
                    }
                } finally {
                    if (!success && null != target) {
                        //Delete claim to not pollute id space
                        for (int attempt = 0; attempt < ROLLBACK_ATTEMPTS; attempt++) {
                            try {
                                final StaticBuffer finalTarget = target; // copy for the inner class
                                BackendOperation.execute(txh -> {
                                    idStore.mutate(partitionKey, KeyColumnValueStore.NO_ADDITIONS, Collections.singletonList(finalTarget), txh);
                                    return true;
                                }, new BackendOperation.TransactionalProvider() { //Use normal consistency level for these non-critical delete operations
                                    @Override
                                    public StoreTransaction openTx() throws BackendException {
                                        return manager.beginTransaction(storeTxConfigBuilder.build());
                                    }
                                    @Override
                                    public void close() {}
                                },times);

                                break;
                            } catch (BackendException e) {
                                log.warn("Storage exception while deleting old block application - retrying in {}", rollbackWaitTime, e);
                                if (!rollbackWaitTime.isZero())
                                    sleepAndConvertInterrupts(rollbackWaitTime);
                            }
                        }
                    }
                }
            } catch (UniqueIDExhaustedException e) {
                // No need to increment the backoff wait time or to sleep
                log.warn(e.getMessage());
            } catch (TemporaryBackendException e) {
                backoffMS = Durations.min(backoffMS.multipliedBy(2), idApplicationWaitMS.multipliedBy(32));
                log.warn("Temporary storage exception while acquiring id block - retrying in {}: {}", backoffMS, e);
                sleepAndConvertInterrupts(backoffMS);
            }
        }

        throw new TemporaryLockingException(String.format("Reached timeout %d (%s elapsed) when attempting to allocate id block on partition(%d)-namespace(%d)",
                                            timeout.getNano(), methodTime, partition, idNamespace));
    }


    private StaticBuffer[] getBlockSlice(long blockValue) {
        StaticBuffer[] slice = new StaticBuffer[2];
        slice[0] = new WriteByteBuffer(16).putLong(-blockValue).putLong(0).getStaticBuffer();
        slice[1] = new WriteByteBuffer(16).putLong(-blockValue).putLong(-1).getStaticBuffer();
        return slice;
    }

    private StaticBuffer getBlockApplication(long blockValue, Instant timestamp) {
        WriteByteBuffer bb = new WriteByteBuffer(
                8 // counter long
                        + 8 // time in ms
                        + uidBytes.length);

        bb.putLong(-blockValue).putLong(times.getTime(timestamp));
        WriteBufferUtil.put(bb, uidBytes);
        return bb.getStaticBuffer();
    }

    private long getBlockValue(Entry column) {
        return -column.getLong(0);
    }

    private void sleepAndConvertInterrupts(Duration d) throws BackendException {
        try {
            times.sleepPast(times.getTime().plus(d));
        } catch (InterruptedException e) {
            throw new PermanentBackendException(e);
        }
    }

    private static class UniqueIDExhaustedException extends Exception {

        private static final long serialVersionUID = 1L;

        public UniqueIDExhaustedException(String msg) {
            super(msg);
        }

    }

    private static class ConsistentKeyIDBlock implements IDBlock {

        private final long startIDCount;
        private final long numIds;
        private final int uniqueIDBitWidth;
        private final int uniqueID;


        private ConsistentKeyIDBlock(long startIDCount, long numIDs, int uniqueIDBitWidth, int uniqueID) {
            this.startIDCount = startIDCount;
            this.numIds = numIDs;
            this.uniqueIDBitWidth = uniqueIDBitWidth;
            this.uniqueID = uniqueID;
        }


        @Override
        public long numIds() {
            return numIds;
        }

        @Override
        public long getId(long index) {
            if (index<0 || index>= numIds) throw new ArrayIndexOutOfBoundsException((int)index);
            assert uniqueID<(1<<uniqueIDBitWidth);
            return ((startIDCount +index)<<uniqueIDBitWidth) + uniqueID;
        }

        @Override
        public String toString() {
            String interval = "["+ startIDCount +","+(startIDCount + numIds)+")";
            if (uniqueIDBitWidth>0) interval+="/"+uniqueID+":"+uniqueIDBitWidth;
            return interval;
        }
    }
}

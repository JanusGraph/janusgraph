package com.thinkaurelius.titan.diskstorage.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.graphdb.database.idassigner.DefaultIDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class OrderedKeyColumnValueIDManager {

    private static final Logger log = LoggerFactory.getLogger(OrderedKeyColumnValueIDManager.class);

    /* This value can't be changed without either
      * corrupting existing ID allocations or taking
      * some additional action to prevent such
      * corruption.
      */
    private static final long BASE_ID = 1;

    private static final ByteBuffer empty = ByteBuffer.allocate(0);



	private final OrderedKeyColumnValueStore store;
	
	private final long lockWaitMS;
	private final int lockRetryCount;
	
	private final byte[] rid;

    private IDBlockSizer blockSizer;
    private volatile boolean isActive;


//    public OrderedKeyColumnValueIDManager(OrderedKeyColumnValueStore store, byte[] rid, int blockSize, long lockWaitMS, int lockRetryCount) {
	public OrderedKeyColumnValueIDManager(OrderedKeyColumnValueStore store, byte[] rid, Configuration config) {

		this.store = store;
		
		this.rid = rid;
		
		this.blockSizer = new DefaultIDBlockSizer(config.getLong(
                GraphDatabaseConfiguration.IDAUTHORITY_BLOCK_SIZE_KEY,
                GraphDatabaseConfiguration.IDAUTHORITY_BLOCK_SIZE_DEFAULT));
        this.isActive = false;

		this.lockWaitMS = 
				config.getLong(
						GraphDatabaseConfiguration.IDAUTHORITY_WAIT_MS_KEY,
						GraphDatabaseConfiguration.IDAUTHORITY_WAIT_MS_DEFAULT);
		
		this.lockRetryCount = 
				config.getInt(
						GraphDatabaseConfiguration.IDAUTHORITY_RETRY_COUNT_KEY,
						GraphDatabaseConfiguration.IDAUTHORITY_RETRY_COUNT_DEFAULT);
	}

    public synchronized void setIDBlockSizer(IDBlockSizer sizer) {
        if (isActive) throw new IllegalStateException("IDBlockSizer cannot be changed after IDAuthority is in use");
        this.blockSizer=sizer;
    }

	public long[] getIDBlock(int partition) throws StorageException {
        isActive=true;
		long blockSize = blockSizer.getBlockSize(partition);

		for (int retry = 0; retry < lockRetryCount; retry++) {

            try {
                // Read the latest counter value from the store
                ByteBuffer partitionKey = ByteBuffer.allocate(4);
                partitionKey.putInt(partition).rewind();
                List<Entry> blocks = store.getSlice(partitionKey, empty, empty, null);

                long latest = BASE_ID - blockSize;

                if (null != blocks) {
                    for (Entry e : blocks) {
                        long counterVal = e.getColumn().getLong();
                        if (latest < counterVal) {
                            latest = counterVal;
                        }
                    }
                }

                // calculate the start (inclusive) and end (exclusive) of the allocation we're about to attempt
                long nextStart = latest + blockSize;
                long nextEnd = nextStart + blockSize;

                ByteBuffer target = getBlockApplication(nextStart);

                // attempt to write our claim on the next id block
                long before = System.currentTimeMillis();
                store.mutate(partitionKey, Arrays.asList(new Entry(target, empty)), null, null);
                long after = System.currentTimeMillis();

                if (lockWaitMS < after - before) {
                    // Too slow
                    // Delete block claim and loop again
                    log.warn("Wrote claim for id block [{}, {}) in {} ms: too slow, retrying",
                            new Object[] { nextStart, nextEnd, after - before });
                    store.mutate(partitionKey, null, Arrays.asList(target), null);
                } else {

                    /* At this point we've written our claim on [nextStart, nextEnd),
                     * but we haven't yet guaranteed the absence of a contending claim on
                     * the same id block from another machine
                     */

                    while (true) {
                        // Wait until lockWaitMS has passed since our claim
                        final long sinceLock = System.currentTimeMillis() - after;
                        if (sinceLock >= lockWaitMS) {
                            break;
                        } else {
                            try {
                                Thread.sleep(lockWaitMS - sinceLock);
                            } catch (InterruptedException e) {
                                throw new PermanentLockingException("Interupted while waiting for lock confirmation",e);
                            }
                        }
                    }

                    assert 0 != target.remaining();

                    // Read all id allocation claims on this partition, for the counter value we're claiming
                    ByteBuffer nextStartBB = ByteBuffer.allocate(8);
                    ByteBuffer nextEndBB = ByteBuffer.allocate(8);
                    nextStartBB.putLong(nextStart).rewind();
                    nextEndBB.putLong(nextEnd).rewind();
                    blocks = store.getSlice(partitionKey, nextStartBB, nextEndBB, null);

                    /* If our claim is the lexicographically last one, then our claim
                     * is the most senior one and we own this id block
                     */
                    if (target.equals(blocks.get(blocks.size() - 1).getColumn())) {

                        long result[] = new long[2];
                        result[0] = nextStart;
                        result[1] = nextEnd;

                        if (log.isDebugEnabled()) {
                            log.debug("Acquired ID block [{},{}) on partition {} (my rid is {})",
                                    new Object[] { nextStart, nextEnd, partition, new String(Hex.encodeHex(rid)) });
                        }

                        return result;
                    }

                    log.debug("Failed to acquire ID block [{},{}) (another host claimed it first)", nextStart, nextEnd);

                    /* Another claimant beat us to this id block -- try again.
                     *
                     * Note that we don't have to delete our failed claim; it
                     * is lexicographically prior to, and therefore considered
                     * junior to, some other machine's claim.  We could delete
                     * it, but doing so isn't necessary.
                     */
                }
            } catch (TemporaryStorageException e) {
                log.debug("Temporary storage exception while acquiring lock: {}",e);
            }
		}
		
		throw new TemporaryLockingException("Exceeded timeout count ["+lockRetryCount+"] when attempting to allocate id block");
    }
	
	private ByteBuffer getBlockApplication(long counter) {
		ByteBuffer bb = ByteBuffer.allocate(
				8 // counter long
				+ 8 // time in ms
				+ rid.length);
		
		bb.putLong(counter).putLong(System.currentTimeMillis()).put(rid);
		bb.rewind();
		return bb;
	}
}

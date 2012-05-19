package com.thinkaurelius.titan.diskstorage.cassandra;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.OrderedKeyColumnValueStore;
import com.thinkaurelius.titan.exceptions.GraphStorageException;

public class CassandraIDManager {
	
	private final OrderedKeyColumnValueStore store;
	
	private final ByteBuffer empty = ByteBuffer.allocate(0);
	
	private final byte[] rid;
	
	private static final long BLOCK_SIZE_OVERRIDE = 1000;
	private static final long BASE_ID = 0;
	
	private static final int lockWaitMS = 500;
	private static final int lockRetryCount = 3;
	
	private static final Logger log = LoggerFactory.getLogger(CassandraIDManager.class);

    public CassandraIDManager(OrderedKeyColumnValueStore store, byte[] rid) {
		this.store = store;
		this.rid = rid;
	}

	public long[] getIDBlock(int partition, int blockSize) {
		
		for (int retry = 0; retry < lockRetryCount; retry++) {
			
			// Read the latest counter value from the store
			ByteBuffer partitionKey = ByteBuffer.allocate(4);
			partitionKey.putInt(partition).rewind();
			List<Entry> blocks = store.getSlice(partitionKey, empty, empty, null);
			
			long latest = BASE_ID;
			
			if (null != blocks) {
				for (Entry e : blocks) {
					long counterVal = e.getColumn().getLong();
					if (latest < counterVal) {
						latest = counterVal;
					}
				}
			}
			
			// calculate the start (inclusive) and end (exclusive) of the allocation we're about to attempt
			long nextStart = latest + BLOCK_SIZE_OVERRIDE;
			long nextEnd = nextStart + BLOCK_SIZE_OVERRIDE;
			
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
				log.debug("Wrote (but have not yet validated) claim for ID block [{},{})", nextStart, nextEnd);
				
				while (true) {
					final long delta = System.currentTimeMillis();
					if (delta >= lockWaitMS) {
						break;
					} else {
						try {
							Thread.sleep(lockWaitMS - delta);
						} catch (InterruptedException e) {
							throw new RuntimeException(e); // different exception type?
						}
					}
				}
				
				blocks = store.getSlice(partitionKey, empty, empty, null);
				
				// if our claim is the lexicographically last one, then we succeeded
				if (target.equals(blocks.get(blocks.size() - 1).getColumn())) {
				
					long result[] = new long[2];
					result[0] = nextStart;
					result[1] = nextEnd;
					return result;
				}
				
				// another claimant beat us to this id block -- try again from the top
			}
		}
		
		throw new GraphStorageException("Exceeded timeout count when attempting to allocate id block");
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

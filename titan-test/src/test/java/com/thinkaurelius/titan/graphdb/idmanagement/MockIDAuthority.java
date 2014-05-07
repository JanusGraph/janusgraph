package com.thinkaurelius.titan.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.util.time.Duration;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.StaticIDBlockSizer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class MockIDAuthority implements IDAuthority {

    private static final int BLOCK_SIZE_LIMIT = Integer.MAX_VALUE;

    private final ConcurrentHashMap<Integer, AtomicLong> ids = new ConcurrentHashMap<Integer, AtomicLong>();
    private IDBlockSizer blockSizer;
    private final int blockSizeLimit;
    private final int delayAcquisitionMS;
    private int[] localPartition = {0, -1};

    public MockIDAuthority() {
        this(100);
    }

    public MockIDAuthority(int blockSize) {
        this(blockSize, BLOCK_SIZE_LIMIT);
    }

    public MockIDAuthority(int blockSize, int blockSizeLimit) {
        this(blockSize, blockSizeLimit, 0);
    }

    public MockIDAuthority(int blockSize, int blockSizeLimit, int delayAcquisitionMS) {
        blockSizer = new StaticIDBlockSizer(blockSize, blockSizeLimit);
        this.blockSizeLimit = blockSizeLimit;
        this.delayAcquisitionMS = delayAcquisitionMS;
        Preconditions.checkArgument(0 <= this.delayAcquisitionMS);
    }

    @Override
    public long[] getIDBlock(int partition, Duration timeout) throws StorageException {
        //Delay artificially
        if (delayAcquisitionMS>0) {
            try {
                Thread.sleep(delayAcquisitionMS);
            } catch (InterruptedException e) {
                throw new TemporaryStorageException(e);
            }
        }
        Integer p = Integer.valueOf(partition);
        long size = blockSizer.getBlockSize(partition);
        AtomicLong id = ids.get(p);
        if (id == null) {
            ids.putIfAbsent(p, new AtomicLong(1));
            id = ids.get(p);
            Preconditions.checkNotNull(id);
        }
        long lowerBound = id.getAndAdd(size);
        if (lowerBound >= blockSizeLimit) {
            throw new IDPoolExhaustedException("Reached partition limit: " + blockSizeLimit);
        }
        return new long[]{lowerBound, Math.min(lowerBound + size, blockSizeLimit)};
    }

    public void setLocalPartition(int[] local) {
        this.localPartition = local;
    }

    @Override
    public List<KeyRange> getLocalIDPartition() throws StorageException {
        StaticBuffer lower = new WriteByteBuffer(4).putInt(localPartition[0]).getStaticBuffer();
        StaticBuffer upper = new WriteByteBuffer(4).putInt(localPartition[1]).getStaticBuffer();
        Preconditions.checkArgument(lower.compareTo(upper)<0, Arrays.toString(localPartition));
        return Lists.newArrayList(new KeyRange(lower, upper));
    }

    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        this.blockSizer = sizer;
    }

    @Override
    public void close() throws StorageException {

    }

    @Override
    public String getUniqueID() {
        return "";
    }
}
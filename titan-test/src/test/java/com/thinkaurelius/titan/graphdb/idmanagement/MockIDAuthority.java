package com.thinkaurelius.titan.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.StaticIDBlockSizer;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class MockIDAuthority implements IDAuthority {

    private static final int BLOCK_SIZE_LIMIT = Integer.MAX_VALUE;

    private final ConcurrentHashMap<Integer, AtomicLong> ids = new ConcurrentHashMap<Integer, AtomicLong>();
    private IDBlockSizer blockSizer = null;
    private int blockSizeLimit = BLOCK_SIZE_LIMIT;
    private int[] localPartition = {0, -1};

    private int delayAcquisitionMS = 0;

    public MockIDAuthority() {
        this(100);
    }

    public MockIDAuthority(int blockSize) {
        this(blockSize, BLOCK_SIZE_LIMIT);
    }

    public MockIDAuthority(int blockSize, int blockSizeLimit) {
        blockSizer = new StaticIDBlockSizer(blockSize,blockSizeLimit);
        this.blockSizeLimit = blockSizeLimit;
    }

    public void setDelayAcquisition(int timeMS) {
        Preconditions.checkArgument(timeMS>=0);
        this.delayAcquisitionMS=timeMS;
    }

    @Override
    public synchronized long[] getIDBlock(int partition) throws StorageException {
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
    public StaticBuffer[] getLocalIDPartition() throws StorageException {
        ByteBuffer lower = ByteBuffer.allocate(4);
        ByteBuffer upper = ByteBuffer.allocate(4);
        lower.putInt(localPartition[0]);
        upper.putInt(localPartition[1]);
        lower.rewind();
        upper.rewind();
        Preconditions.checkArgument(ByteBufferUtil.isSmallerThan(lower, upper), Arrays.toString(localPartition));
        return new StaticBuffer[]{new StaticByteBuffer(lower), new StaticByteBuffer(upper)};
    }

    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        this.blockSizer = sizer;
    }

    @Override
    public void close() throws StorageException {

    }
}
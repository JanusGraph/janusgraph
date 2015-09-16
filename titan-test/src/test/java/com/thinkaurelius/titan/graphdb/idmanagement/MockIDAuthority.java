package com.thinkaurelius.titan.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.diskstorage.*;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyRange;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDPoolExhaustedException;
import com.thinkaurelius.titan.graphdb.database.idassigner.StaticIDBlockSizer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class MockIDAuthority implements IDAuthority {

    private static final int BLOCK_SIZE_LIMIT = Integer.MAX_VALUE;

    private final ConcurrentHashMap<Long, AtomicLong> ids = new ConcurrentHashMap<Long, AtomicLong>();
    private IDBlockSizer blockSizer;
    private final int blockSizeLimit;
    private final int delayAcquisitionMS;
    private List<KeyRange> localPartition = null;

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
    public IDBlock getIDBlock(final int partition, final int idNamespace, Duration timeout) throws BackendException {
        //Delay artificially
        if (delayAcquisitionMS>0) {
            try {
                Thread.sleep(delayAcquisitionMS);
            } catch (InterruptedException e) {
                throw new TemporaryBackendException(e);
            }
        }
        Preconditions.checkArgument(partition>=0 && partition<=Integer.MAX_VALUE);
        Preconditions.checkArgument(idNamespace>=0 && idNamespace<=Integer.MAX_VALUE);
        Long p = (((long)partition)<<Integer.SIZE) + ((long)idNamespace);
        long size = blockSizer.getBlockSize(idNamespace);
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
        return new MockIDBlock(lowerBound,Math.min(size,blockSizeLimit-lowerBound));
    }

    private static class MockIDBlock implements IDBlock {

        private final long start;
        private final long numIds;

        private MockIDBlock(long start, long numIds) {
            this.start = start;
            this.numIds = numIds;
        }

        @Override
        public long numIds() {
            return numIds;
        }

        @Override
        public long getId(long index) {
            if (index<0 || index>=numIds) throw new ArrayIndexOutOfBoundsException((int)index);
            return start+index;
        }
    }

    public void setLocalPartition(List<KeyRange> local) {
        this.localPartition = local;
    }

    @Override
    public List<KeyRange> getLocalIDPartition() throws BackendException {
        Preconditions.checkNotNull(localPartition);
        return localPartition;
    }

    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        this.blockSizer = sizer;
    }

    @Override
    public void close() throws BackendException {

    }

    @Override
    public String getUniqueID() {
        return "";
    }

    @Override
    public boolean supportsInterruption()
    {
        return true;
    }
}
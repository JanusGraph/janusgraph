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

package org.janusgraph.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.IDBlock;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;
import org.janusgraph.graphdb.database.idassigner.IDPoolExhaustedException;
import org.janusgraph.graphdb.database.idassigner.StaticIDBlockSizer;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class MockIDAuthority implements IDAuthority {

    private static final int BLOCK_SIZE_LIMIT = Integer.MAX_VALUE;

    private final ConcurrentHashMap<Long, AtomicLong> ids = new ConcurrentHashMap<>();
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
        Preconditions.checkArgument(partition >= 0);
        Preconditions.checkArgument(idNamespace >= 0);
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
    public List<KeyRange> getLocalIDPartition() {
        Preconditions.checkNotNull(localPartition);
        return localPartition;
    }

    @Override
    public void setIDBlockSizer(IDBlockSizer sizer) {
        this.blockSizer = sizer;
    }

    @Override
    public void close() {

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

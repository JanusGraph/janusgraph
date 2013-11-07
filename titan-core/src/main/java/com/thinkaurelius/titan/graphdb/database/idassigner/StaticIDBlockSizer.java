package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StaticIDBlockSizer implements IDBlockSizer {

    private final long blockSize;
    private final long blockSizeLimit;

    public StaticIDBlockSizer(long blockSize, long blockSizeLimit) {
        Preconditions.checkArgument(blockSize > 0);
        Preconditions.checkArgument(blockSizeLimit > 0);
        Preconditions.checkArgument(blockSizeLimit > blockSize,"%s vs %s",blockSizeLimit,blockSize);
        this.blockSize = blockSize;
        this.blockSizeLimit = blockSizeLimit;
    }

    @Override
    public long getBlockSize(int partitionID) {
        return blockSize;
    }

    @Override
    public long getIdUpperBound(int partitionID) {
        return blockSizeLimit;
    }
}

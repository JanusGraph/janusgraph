package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StaticIDBlockSizer implements IDBlockSizer {

    private final long blockSize;

    public StaticIDBlockSizer(long blockSize) {
        Preconditions.checkArgument(blockSize > 0);
        this.blockSize = blockSize;
    }

    @Override
    public long getBlockSize(int partitionID) {
        return blockSize;
    }
}

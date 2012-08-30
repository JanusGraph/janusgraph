package com.thinkaurelius.titan.graphdb.database.idassigner;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DefaultIDBlockSizer implements IDBlockSizer {
    
    private final long blockSize;
    
    public DefaultIDBlockSizer() {
        this(GraphDatabaseConfiguration.IDAUTHORITY_BLOCK_SIZE_DEFAULT);
    }

    public DefaultIDBlockSizer(long blockSize) {
        Preconditions.checkArgument(blockSize>0);
        this.blockSize=blockSize;
    }
    
    @Override
    public long getBlockSize(int partitionID) {
        return blockSize;
    }
}

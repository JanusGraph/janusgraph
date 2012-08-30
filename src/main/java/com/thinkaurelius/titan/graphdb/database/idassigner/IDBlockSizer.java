package com.thinkaurelius.titan.graphdb.database.idassigner;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface IDBlockSizer {
    
    public long getBlockSize(int partitionID);
    
}

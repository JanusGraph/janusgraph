package com.thinkaurelius.titan.graphdb.database.idassigner;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface IDBlockSizer {

    public long getBlockSize(int partitionID);

}

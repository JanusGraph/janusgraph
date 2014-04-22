package com.thinkaurelius.titan.diskstorage;

import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LoggableTransaction extends TransactionHandle {

    public void logMutations(DataOutput out);

}

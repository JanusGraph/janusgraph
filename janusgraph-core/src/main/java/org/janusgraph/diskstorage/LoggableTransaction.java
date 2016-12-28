package org.janusgraph.diskstorage;

import org.janusgraph.graphdb.database.serialize.DataOutput;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface LoggableTransaction extends BaseTransaction {

    public void logMutations(DataOutput out);

}

package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimplePartitionAssignment implements PartitionAssignment {

    private int partitionID;

    public SimplePartitionAssignment() {
    }

    public SimplePartitionAssignment(int id) {
        setPartitionID(id);
    }

    public void setPartitionID(int id) {
        Preconditions.checkArgument(id >= 0);
        this.partitionID = id;
    }

    @Override
    public int getPartitionID() {
        return partitionID;
    }
}

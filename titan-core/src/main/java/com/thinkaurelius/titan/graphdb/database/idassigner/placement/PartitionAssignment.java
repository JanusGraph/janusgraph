package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface PartitionAssignment {

    public static final PartitionAssignment EMPTY = new PartitionAssignment() {
        @Override
        public int getPartitionID() {
            return -1;
        }
    };

    public int getPartitionID();

}

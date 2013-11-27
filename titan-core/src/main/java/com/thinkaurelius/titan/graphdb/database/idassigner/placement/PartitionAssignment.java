package com.thinkaurelius.titan.graphdb.database.idassigner.placement;

/**
 * Utility interface used in {@link IDPlacementStrategy} to hold the partition assignment of
 * a vertex (if it is already assigned a partition) or to be assigned a partition id.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface PartitionAssignment {

    /**
     * Default assignment (when no id has been assigned yet)
     */
    public static final PartitionAssignment EMPTY = new PartitionAssignment() {
        @Override
        public int getPartitionID() {
            return -1;
        }
    };

    /**
     * Returns the assigned partition id
     * @return
     */
    public int getPartitionID();

}

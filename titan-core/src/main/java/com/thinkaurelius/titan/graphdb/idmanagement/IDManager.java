package com.thinkaurelius.titan.graphdb.idmanagement;


import com.google.common.base.Preconditions;

/**
 * Handles the allocation of ids based on the type of element
 * Responsible for the bit-wise pattern of Titan's internal id scheme.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IDManager {

    //public static final long MaxEntityID = Long.MAX_VALUE>>2;
    public enum IDType {
        Relation {
            @Override
            public final long offset() {
                return 1l;
            }

            @Override
            public final long id() {
                return 1l;
            } // 1b
        },
        TitanType {
            @Override
            public final long offset() {
                return 2l;
            }

            @Override
            public final long id() {
                return 2l;
            } // 10b
        },
        PropertyKey {
            @Override
            public final long offset() {
                return 3l;
            }

            @Override
            public final long id() {
                return 2l;
            }    // 010b
        },
        EdgeLabel {
            @Override
            public final long offset() {
                return 3l;
            }

            @Override
            public final long id() {
                return 6l;
            } // 110b
        },
        Vertex {
            @Override
            public final long offset() {
                return 2l;
            }

            @Override
            public final long id() {
                return 0l;
            } // 00b
        };


        public abstract long offset();

        public abstract long id();

        public final long addPadding(long id) {
            return (id << offset()) | id();
        }

        public final long removePadding(long id) {
            return id >>> offset();
        }

        public final boolean is(long id) {
            return (id & ((1l << offset()) - 1)) == id();
        }
    }

    /**
     * Number of bits that need to be reserved from the type ids for storing additional information during serialization
     */
    public static final int TYPE_LEN_RESERVE = 2;

    /**
     * Total number of bits available to a Titan assigned id
     * We use only 63 bits to make sure that all ids are positive
     *
     * @see com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler#getKey(long)
     */
    private static final long TOTAL_BITS = 63;

    /**
     * Maximum number of bits that can be used for the partition prefix of an id
     */
    private static final long MAX_PARTITION_BITS = 31;
    /**
     * Default number of bits used for the partition prefix. 0 means there is no partition prefix
     */
    private static final long DEFAULT_PARTITION_BITS = 0;

    /**
     * Maximum id of any titan type
     */
    private static final long MAX_TITAN_TYPE_ID = (1l << (TOTAL_BITS - IDType.EdgeLabel.offset()
            - TYPE_LEN_RESERVE)) - 1;


    @SuppressWarnings("unused")
    private final long partitionBits;
    private final long partitionOffset;
    private final long maxPartitionID;

    private final long maxRelationID;
    private final long maxVertexID;


    public IDManager(long partitionBits) {
        Preconditions.checkArgument(partitionBits >= 0);
        Preconditions.checkArgument(partitionBits <= MAX_PARTITION_BITS,
                "Partition bits can be at most %s bits", MAX_PARTITION_BITS);
        this.partitionBits = partitionBits;

        maxPartitionID = (1l << (partitionBits)) - 1;

        maxRelationID = (1l << (TOTAL_BITS - partitionBits - IDType.Relation.offset())) - 1;
        maxVertexID = (1l << (TOTAL_BITS - partitionBits - IDType.Vertex.offset())) - 1;

        partitionOffset = TOTAL_BITS - partitionBits;
    }

    public IDManager() {
        this(DEFAULT_PARTITION_BITS);
    }

    private static long prefixWithOffset(long id, long prefixid, long prefixOffset, long maxPrefixID) {
        assert maxPrefixID >= 0 && prefixOffset < 64;
        if (id < 0) throw new IllegalArgumentException("ID cannot be negative: " + id);
        if (prefixid < 0) throw new IllegalArgumentException("Prefix ID cannot be negative: " + prefixid);
        if (prefixid == 0) return id;
        if (prefixid > maxPrefixID)
            throw new IllegalArgumentException("Prefix ID exceeds limit of: " + maxPrefixID);
        assert id < (1l << prefixOffset) : "ID is too large for prefix offset: " + id + " ( " + prefixOffset + " )";
        return (prefixid << prefixOffset) | id;
    }


    private long addPartition(long id, long partitionID) {
        assert id > 0;
        assert partitionID >= 0;
        return prefixWithOffset(id, partitionID, partitionOffset, maxPartitionID);
    }

    /*		--- TitanElement id bit format ---
      *  [ 0 | partitionID | count | ID padding ]
     */


    public long getRelationID(long count, long partition) {
        if (count < 0 || count > maxRelationID)
            throw new IllegalArgumentException("Invalid count for bound:" + maxRelationID);
        return addPartition(IDType.Relation.addPadding(count), partition);
    }


    public long getVertexID(long count, long partition) {
        if (count < 0 || count > maxVertexID)
            throw new IllegalArgumentException("Invalid count for bound:" + maxVertexID);
        return addPartition(IDType.Vertex.addPadding(count), partition);
    }


    /* --- TitanRelation Type id bit format ---
      *  [ 0 | count | ID padding ]
     */

    public static long getEdgeLabelID(long count) {
        assert count > 0 && count < MAX_TITAN_TYPE_ID;
        return IDType.EdgeLabel.addPadding(count);
    }

    public static long getPropertyKeyID(long count) {
        Preconditions.checkArgument(count > 0 && count < MAX_TITAN_TYPE_ID,
                "Invalid count [%s] for bound: %s", count, MAX_TITAN_TYPE_ID);
        if (count < 0 || count > MAX_TITAN_TYPE_ID)
            throw new IllegalArgumentException("Invalid count for bound:" + MAX_TITAN_TYPE_ID);
        return IDType.PropertyKey.addPadding(count);
    }

    public static long getTypeCount(long typeid) {
        Preconditions.checkArgument(IDType.TitanType.is(typeid));
        return IDType.EdgeLabel.removePadding(typeid);
    }


    public long getMaxRelationCount() {
        return maxRelationID;
    }

    public long getMaxTitanTypeCount() {
        return MAX_TITAN_TYPE_ID;
    }

    public long getMaxVertexCount() {
        return maxVertexID;
    }

    public long getMaxPartitionCount() {
        return maxPartitionID;
    }


    public long getPartitionID(long id) {
        Preconditions.checkArgument(!IDType.TitanType.is(id), "Types don't have a partition: %s", id);
        return (id >>> partitionOffset);
    }

    public long isolatePartitionID(long id) {
        return getPartitionID(id) << partitionOffset;
    }

    public static final boolean isVertexID(long id) {
        return IDType.Vertex.is(id);
    }

    public static final boolean isTypeID(long id) {
        return IDType.TitanType.is(id);
    }

    public static final boolean isPropertyKeyID(long id) {
        return IDType.PropertyKey.is(id);
    }

    public static final boolean isEdgeLabelID(long id) {
        return IDType.EdgeLabel.is(id);
    }

    public static final boolean isRelationID(long id) {
        return IDType.Relation.is(id);
    }

    private final IDInspector inspector = new IDInspector() {

        @Override
        public final boolean isRelationID(long id) {
            return IDManager.isRelationID(id);
        }

        @Override
        public final boolean isTypeID(long id) {
            return IDManager.isTypeID(id);
        }

        @Override
        public final boolean isEdgeLabelID(long id) {
            return IDManager.isEdgeLabelID(id);
        }

        @Override
        public final boolean isPropertyKeyID(long id) {
            return IDManager.isPropertyKeyID(id);
        }

        @Override
        public final boolean isVertexID(long id) {
            return IDManager.isVertexID(id);
        }

        @Override
        public final long getPartitionID(long id) {
            return IDManager.this.getPartitionID(id);
        }
    };

    public IDInspector getIDInspector() {
        return inspector;
    }

}

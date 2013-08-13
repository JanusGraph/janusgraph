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

    /** Number of bits that need to be reserved from the type ids for storing additional information during serialization
     */
    public static final int TYPE_LEN_RESERVE = 2;

    /**Total number of bits available to a Titan assigned id
        We use only 63 bits to make sure that all ids are positive
     */
    private static final long TOTAL_BITS = 63;

    private static final long MAX_PARTITION_BITS = 31;
    private static final long DEFAULT_PARTITION_BITS = 0;

    private static final long MAX_TITAN_TYPE_ID = (1l << (TOTAL_BITS - IDType.EdgeLabel.offset()
            - TYPE_LEN_RESERVE)) - 1;

//    public static final long relationTypePaddingBits = 3; //??


//    private static final long titanTypeDirectionOffset = totalNoBits - relationTypeDirectionBits;
//    private static final long titanTypeDirectionMask = ((1l << relationTypeDirectionBits) - 1) << (titanTypeDirectionOffset);

//    private static final long maxDirectionID = (1l << relationTypeDirectionBits) - 2; //Need extra number for bounding

//    public static final long titanTypePaddingMask = (1l << relationTypePaddingBits) - 1;
//    public static final long titanTypePaddingFrontOffset = titanTypeDirectionOffset - relationTypePaddingBits;
//    public static final long titanTypePaddingFrontMask = titanTypePaddingMask << titanTypePaddingFrontOffset;

    @SuppressWarnings("unused")
    private final long partitionBits;
    private final long partitionOffset;
    private final long maxPartitionID;

    private final long maxRelationID;
    private final long maxVertexID;

//    private final long groupIDMask;
//    private final long inverseGroupIDMask;
//
//    private final long groupIDMaskTMP;
//    private final long groupIDFrontMask;
//    private final long groupIDDeltaOffset;
//    private final long titanTypeIDBackLength;
//    private final long titanTypeCountPartMask;



    public IDManager(long partitionBits) {
        Preconditions.checkArgument(partitionBits >= 0);
        Preconditions.checkArgument(partitionBits <= MAX_PARTITION_BITS,
            "Partition bits can be at most %s bits", MAX_PARTITION_BITS);
        this.partitionBits = partitionBits;

        maxPartitionID = (1l << (partitionBits)) - 1;

        maxRelationID = (1l << (TOTAL_BITS - partitionBits - IDType.Relation.offset())) - 1;
        maxVertexID = (1l << (TOTAL_BITS - partitionBits - IDType.Vertex.offset())) - 1;

        partitionOffset = TOTAL_BITS - partitionBits;
//        groupOffset = partitionOffset - groupBits - relationTypeDirectionBits;
//
//        groupIDMask = (1l << (groupOffset + groupBits)) - (1l << groupOffset);
//        inverseGroupIDMask = ~groupIDMask;

//        titanTypeIDBackLength = relationTypePaddingBits + relationTypeDirectionBits;
//        groupIDMaskTMP = ((1l << groupBits) - 1) << relationTypePaddingBits;
//        groupIDDeltaOffset = titanTypePaddingFrontOffset - groupBits - relationTypePaddingBits;
//        groupIDFrontMask = groupIDMaskTMP << groupIDDeltaOffset;

//        titanTypeCountPartMask = (1l << (totalNoBits - titanTypeIDBackLength)) - 1;

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

    public final static long getEdgeLabelID(long count) {
        Preconditions.checkArgument(count>0 && count< MAX_TITAN_TYPE_ID,
                "Invalid count [%s] for bound: %s",count, MAX_TITAN_TYPE_ID);
        return IDType.EdgeLabel.addPadding(count);
    }

    public final static long getPropertyKeyID(long count) {
        Preconditions.checkArgument(count>0 && count< MAX_TITAN_TYPE_ID,
                "Invalid count [%s] for bound: %s",count, MAX_TITAN_TYPE_ID);
        if (count < 0 || count > MAX_TITAN_TYPE_ID)
            throw new IllegalArgumentException("Invalid count for bound:" + MAX_TITAN_TYPE_ID);
        return IDType.PropertyKey.addPadding(count);
    }

    public final static long getTypeCount(long typeid) {
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
        Preconditions.checkArgument(!IDType.TitanType.is(id),"Types don't have a partition: %s",id);
        return (id >>> partitionOffset);
    }

    public long isolatePartitionID(long id) {
        return getPartitionID(id) << partitionOffset;
    }

    public static boolean isVertexID(long id) {
        return IDType.Vertex.is(id);
    }

    public static boolean isTypeID(long id) {
        return IDType.TitanType.is(id);
    }

    public static boolean isPropertyKeyID(long id) {
        return IDType.PropertyKey.is(id);
    }

    public static boolean isEdgeLabelID(long id) {
        return IDType.EdgeLabel.is(id);
    }

    public static boolean isRelationID(long id) {
        return IDType.Relation.is(id);
    }

    private final IDInspector inspector = new IDInspector() {

        @Override
        public boolean isRelationID(long id) {
            return IDManager.isRelationID(id);
        }

        @Override
        public boolean isTypeID(long id) {
            return IDManager.isTypeID(id);
        }

        @Override
        public boolean isEdgeLabelID(long id) {
            return IDManager.isEdgeLabelID(id);
        }

        @Override
        public boolean isPropertyKeyID(long id) {
            return IDManager.isPropertyKeyID(id);
        }

        @Override
        public boolean isVertexID(long id) {
            return IDManager.isVertexID(id);
        }

        @Override
        public long getPartitionID(long id) {
            return IDManager.this.getPartitionID(id);
        }
    };

    public IDInspector getIDInspector() {
        return inspector;
    }

}

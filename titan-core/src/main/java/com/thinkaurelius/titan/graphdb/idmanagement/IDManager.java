package com.thinkaurelius.titan.graphdb.idmanagement;


import com.google.common.base.Preconditions;

/**
 * Handles the allocation of ids based on the type of element
 * Responsible for the bit-wise pattern of Titan's internal id scheme.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IDManager implements IDInspector {

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

        public final boolean is(long id) {
            return (id & ((1l << offset()) - 1)) == id();
        }
    }

    public static final long totalNoBits = 63;
    public static final long relationTypeDirectionBits = 2;
    public static final long relationTypePaddingBits = 3;
    public static final long minimumCountBits = 4;

    public static final long maxGroupBits = 31;
    public static final long maxPartitionBits = 31;

    public static final long defaultPartitionBits = 0;
    public static final long defaultGroupBits = 6;

    private static final long titanTypeDirectionOffset = totalNoBits - relationTypeDirectionBits;
    private static final long titanTypeDirectionMask = ((1l << relationTypeDirectionBits) - 1) << (titanTypeDirectionOffset);

    private static final long maxDirectionID = (1l << relationTypeDirectionBits) - 2; //Need extra number for bounding

    public static final long titanTypePaddingMask = (1l << relationTypePaddingBits) - 1;
    public static final long titanTypePaddingFrontOffset = titanTypeDirectionOffset - relationTypePaddingBits;
    public static final long titanTypePaddingFrontMask = titanTypePaddingMask << titanTypePaddingFrontOffset;

    @SuppressWarnings("unused")
    private final long partitionBits;
    private final long groupBits;

    private final long partitionOffset;
    private final long groupOffset;

    private final long groupIDMask;
    private final long inverseGroupIDMask;

    private final long groupIDMaskTMP;
    private final long groupIDFrontMask;
    private final long groupIDDeltaOffset;
    private final long titanTypeIDBackLength;
    private final long titanTypeCountPartMask;

    private final long maxGroupID;
    private final long maxRelationID;
    private final long maxTitanTypeID;
    private final long maxVertexID;

    private final long maxPartitionID;

    public IDManager(long partitionBits, long groupBits) {
        Preconditions.checkArgument(partitionBits >= 0);
        Preconditions.checkArgument(groupBits >= 0);
        if (partitionBits > maxPartitionBits)
            throw new IllegalArgumentException("Partition bits can be at most " + maxPartitionBits);
        if (groupBits > maxGroupBits)
            throw new IllegalArgumentException("Group bits can be at most " + maxGroupBits);
        if (totalNoBits - partitionBits - 1 < minimumCountBits)
            throw new IllegalArgumentException("No bits left for id");
        if (totalNoBits - partitionBits - groupBits - 1 < minimumCountBits)
            throw new IllegalArgumentException("Too many group bits");
        this.partitionBits = partitionBits;
        this.groupBits = groupBits;

        maxPartitionID = (1l << (partitionBits)) - 1;
        maxGroupID = (1l << groupBits) - 2; //Need extra number for bounding

        maxRelationID = (1l << (totalNoBits - partitionBits - IDType.Relation.offset())) - 1;
        maxTitanTypeID = (1l << (totalNoBits - partitionBits - IDType.EdgeLabel.offset()
                - groupBits - relationTypeDirectionBits)) - 1;
        maxVertexID = (1l << (totalNoBits - partitionBits - IDType.Vertex.offset())) - 1;

        partitionOffset = totalNoBits - partitionBits;
        groupOffset = partitionOffset - groupBits - relationTypeDirectionBits;

        groupIDMask = (1l << (groupOffset + groupBits)) - (1l << groupOffset);
        inverseGroupIDMask = ~groupIDMask;

        titanTypeIDBackLength = relationTypePaddingBits + groupBits + relationTypeDirectionBits;
        groupIDMaskTMP = ((1l << groupBits) - 1) << relationTypePaddingBits;
        groupIDDeltaOffset = titanTypePaddingFrontOffset - groupBits - relationTypePaddingBits;
        groupIDFrontMask = groupIDMaskTMP << groupIDDeltaOffset;

        titanTypeCountPartMask = (1l << (totalNoBits - titanTypeIDBackLength)) - 1;

    }

    public IDManager() {
        this(defaultPartitionBits, defaultGroupBits);
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

    private long addGroup(long id, long groupID) {
        assert id > 0;
        return prefixWithOffset(id, groupID, groupOffset, maxGroupID);
    }

    @Override
    public boolean isValidTypGroupID(long groupID) {
        return groupID > 0 && groupID <= maxGroupID;
    }

    /*		    	--- TitanElement id bit format ---
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


    /*		    	--- TitanRelation Type id bit format ---
      *  [ 0 | partitionID | directionBits (=0) | groupID | count | ID padding ]
     */

    public long getEdgeLabelID(long count, long groupid, long partition) {
        if (count < 0 || count > maxTitanTypeID)
            throw new IllegalArgumentException("Invalid count for bound:" + maxTitanTypeID);
        return addPartition(addGroup(IDType.EdgeLabel.addPadding(count), groupid), partition);
    }

    public long getPropertyKeyID(long count, long groupid, long partition) {
        if (count < 0 || count > maxTitanTypeID)
            throw new IllegalArgumentException("Invalid count for bound:" + maxTitanTypeID);
        return addPartition(addGroup(IDType.PropertyKey.addPadding(count), groupid), partition);
    }

    @Override
    public long getGroupID(long edgetypeid) {
        return (edgetypeid & groupIDMask) >> groupOffset;
    }

    public long removeGroupID(long edgetypeid) {
        return edgetypeid & inverseGroupIDMask;
    }

    public long addGroupID(long edgetypeid, long groupid) {
        assert getGroupID(edgetypeid) == 0 : "Seems to already have a groupid: " + edgetypeid;
        return (groupid << groupOffset) | edgetypeid;
    }


    public long getGroupBits() {
        return groupBits;
    }

    public long getMaxGroupID() {
        return maxGroupID;
    }

    public long getMaxRelationID() {
        return maxRelationID;
    }

    public long getMaxTitanTypeID() {
        return maxTitanTypeID;
    }

    public long getMaxVertexID() {
        return maxVertexID;
    }

    public long getMaxPartitionID() {
        return maxPartitionID;
    }


    @Override
    public long getPartitionID(long id) {
        return (id >>> partitionOffset);
    }

    public long isolatePartitionID(long id) {
        return getPartitionID(id) << partitionOffset;
    }

    @Override
    public boolean isVertexID(long id) {
        return IDType.Vertex.is(id);
    }

    @Override
    public boolean isTypeID(long id) {
        return IDType.TitanType.is(id);
    }

    @Override
    public boolean isPropertyKeyID(long id) {
        return IDType.PropertyKey.is(id);
    }

    @Override
    public boolean isEdgeLabelID(long id) {
        return IDType.EdgeLabel.is(id);
    }

    @Override
    public boolean isRelationID(long id) {
        return IDType.Relation.is(id);
    }


    public final static long getSystemEdgeLabelID(long id) {
        //assumes groupid=0 and partitionid=0
        return IDManager.IDType.EdgeLabel.addPadding(id);
    }

    public final static long getSystemPropertyKeyID(long id) {
        //assumes groupid=0 and partitionid=0
        return IDManager.IDType.PropertyKey.addPadding(id);
    }

}

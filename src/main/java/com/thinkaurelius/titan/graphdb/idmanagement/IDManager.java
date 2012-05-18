package com.thinkaurelius.titan.graphdb.idmanagement;




public class IDManager implements IDInspector {
	
	//public static final long MaxEntityID = Long.MAX_VALUE>>2;
	public enum IDType {
		Edge {
			@Override
			public final long offset() { return 1l;}
			@Override
			public final long id() { return 1l;} // 1b
		},
        EdgeType {
            @Override
            public final long offset() { return 2l;}
            @Override
            public final long id() { return 2l;} // 10b
        },
		PropertyType {
			@Override
			public final long offset() { return 3l;}
			@Override
			public final long id() { return 2l;}	// 010b		
		},
		RelationshipType {
			@Override
			public final long offset() { return 3l;}
			@Override
			public final long id() { return 6l;} // 110b
		},
		Node {
			@Override
			public final long offset() { return 2l;}
			@Override
			public final long id() { return 0l;} // 00b
		};


		public abstract long offset();
		public abstract long id();
		public final long addPadding(long id) {
			return (id<<offset()) | id();
		}
		public final boolean is(long id) {
			return (id&((1l<<offset())-1)) == id();
		}
	}

	public static final long totalNoBits = 63;
    public static final long edgeTypeDirectionBits = 2;
    public static final long edgeTypePaddingBits = 3;
	public static final long minimumCountBits = 4;

	public static final long maxGroupBits = 31;
	public static final long maxPartitionBits = 31;

	public static final long defaultPartitionBits = 0;
	public static final long defaultGroupBits = 6;
	
	private static final long edgeTypeDirectionOffset=totalNoBits-edgeTypeDirectionBits;
	private static final long edgeTypeDirectionMask  = ((1l<<edgeTypeDirectionBits)-1) << (edgeTypeDirectionOffset);
	
	private static final long maxDirectionID = (1l<<edgeTypeDirectionBits)-2; //Need extra number for bounding

	public static final long edgeTypePaddingMask = (1l<<edgeTypePaddingBits)-1;
	public static final long edgeTypePaddingFrontOffset = edgeTypeDirectionOffset-edgeTypePaddingBits;
	public static final long edgeTypePaddingFrontMask = edgeTypePaddingMask<<edgeTypePaddingFrontOffset;
	
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
	private final long edgeTypeIDBackLength;
	private final long edgeTypeCountPartMask;
	
	private final long maxGroupID;
	private final long maxEdgeID;
	private final long maxEdgeTypeID;
	private final long maxNodeID;
	
	private final long maxPartitionID;

	public IDManager(long partitionBits, long groupBits) {
		if (partitionBits>maxPartitionBits)
			throw new IllegalArgumentException("Partition bits can be at most " + maxPartitionBits);
		if (groupBits>maxGroupBits)
			throw new IllegalArgumentException("Group bits can be at most " + maxGroupBits);
		if (totalNoBits-partitionBits-1<minimumCountBits)
			throw new IllegalArgumentException("No bits left for id");
		if (totalNoBits-partitionBits-groupBits-1<minimumCountBits)
			throw new IllegalArgumentException("Too many group bits");
		this.partitionBits=partitionBits;
		this.groupBits=groupBits;

        maxPartitionID= (1l<<(partitionBits))-1;
		maxGroupID = (1l<<groupBits)-2; //Need extra number for bounding

		maxEdgeID = (1l<<(totalNoBits- IDType.Edge.offset()))-1;
		maxEdgeTypeID = (1l<<(totalNoBits-partitionBits- IDType.RelationshipType.offset()
										 -groupBits-edgeTypeDirectionBits))-1;
		maxNodeID = (1l<<(totalNoBits-partitionBits- IDType.Node.offset()))-1;

		partitionOffset = totalNoBits-partitionBits;
        groupOffset = partitionOffset - groupBits - edgeTypeDirectionBits;

        groupIDMask = (1l<<(groupOffset+groupBits))  - (1l<<groupOffset);
        inverseGroupIDMask = ~groupIDMask;

		edgeTypeIDBackLength=edgeTypePaddingBits + groupBits + edgeTypeDirectionBits;
		groupIDMaskTMP = ((1l<<groupBits)-1) << edgeTypePaddingBits;
		groupIDDeltaOffset = edgeTypePaddingFrontOffset-groupBits-edgeTypePaddingBits;
		groupIDFrontMask = groupIDMaskTMP << groupIDDeltaOffset;
		
		edgeTypeCountPartMask = (1l<<(totalNoBits-edgeTypeIDBackLength))-1;

	}

    public IDManager() {
        this(defaultPartitionBits,defaultGroupBits);
    }

	private static long prefixWithOffset(long id, long prefixid, long prefixOffset, long maxPrefixID) {
        assert maxPrefixID>=0 && prefixOffset<64;
        if (id<0) throw new IllegalArgumentException("ID cannot be negative: " + id);
        if (prefixid<0) throw new IllegalArgumentException("Prefix ID cannot be negative: " + prefixid);
        if (prefixid==0) return id;
        if (prefixid>maxPrefixID)
            throw new IllegalArgumentException("Prefix ID exceeds limit of: " + maxPrefixID);
        assert id<(1l<<prefixOffset) : "ID is too large for prefix offset: " + id + " ( " + prefixOffset + " )";
        return (prefixid<<prefixOffset) | id;
    }


	private long addPartition(long id, long partitionID) {
        assert id>0;
        assert partitionID>=0;
        return prefixWithOffset(id,partitionID,partitionOffset,maxPartitionID);
	}
	
	private long addGroup(long id, long groupID) {
        assert id>0;
        return prefixWithOffset(id,groupID,groupOffset,maxGroupID);
	}

    @Override
	public boolean isValidEdgeTypGroupID(long groupID) {
		return groupID>0 && groupID<=maxGroupID;
	}

    /*		    	--- TitanElement id bit format ---
      *  [ 0 | partitionID | count | ID padding ]
     */


	public long getEdgeID(long count) {
		if (count<0 || count>maxEdgeID) 
			throw new IllegalArgumentException("Invalid count for bound:" + maxEdgeID);
		return IDType.Edge.addPadding(count);
	}


    public long getNodeID(long count, long partition) {
        if (count<0 || count>maxNodeID)
            throw new IllegalArgumentException("Invalid count for bound:" + maxNodeID);
        return addPartition(IDType.Node.addPadding(count), partition);
    }


    /*		    	--- TitanRelation Type id bit format ---
      *  [ 0 | partitionID | directionBits (=0) | groupID | count | ID padding ]
     */

	public long getRelationshipTypeID(long count, long groupid, long partition) {
		if (count<0 || count>maxEdgeTypeID) 
			throw new IllegalArgumentException("Invalid count for bound:" + maxEdgeTypeID);
        return addPartition(addGroup(IDType.RelationshipType.addPadding(count), groupid), partition);
	}
	
	public long getPropertyTypeID(long count, long groupid, long partition) {
		if (count<0 || count>maxEdgeTypeID) 
			throw new IllegalArgumentException("Invalid count for bound:" + maxEdgeTypeID);
        return addPartition(addGroup(IDType.PropertyType.addPadding(count), groupid), partition);
	}

	public long getGroupID(long edgetypeid) {
        return (edgetypeid & groupIDMask)>>groupOffset;
    }

    public long removeGroupID(long edgetypeid) {
        return edgetypeid & inverseGroupIDMask;
    }

    public long addGroupID(long edgetypeid, long groupid) {
        assert getGroupID(edgetypeid)==0 : "Seems to already have a groupid: " + edgetypeid;
        return (groupid<<groupOffset) | edgetypeid;
    }


	public long getGroupBits() {
        return groupBits;
    }
	
	public long getMaxGroupID() {
		return maxGroupID;
	}
	
	public long getMaxEdgeID() {
		return maxEdgeID;
	}
	
	public long getMaxEdgeTypeID() {
		return maxEdgeTypeID;
	}
	
	public long getMaxNodeID() {
		return maxNodeID;
	}
	
	public long getMaxPartitionID() {
		return maxPartitionID;
	}
	
	
	
	@Override
	public long getPartitionID(long id) {
		assert !isEdgeID(id);
		return (id>>>partitionOffset);
	}

    public long isolatePartitionID(long id) {
        return getPartitionID(id)<<partitionOffset;
    }

	@Override
	public boolean isNodeID(long id) {
		return IDType.Node.is(id);
	}

	@Override
	public boolean isEdgeTypeID(long id) {
		return IDType.EdgeType.is(id);
	}
	
	@Override
	public boolean isPropertyTypeID(long id) {
		return IDType.PropertyType.is(id);
	}

	@Override
	public boolean isRelationshipTypeID(long id) {
		return IDType.RelationshipType.is(id);
	}

	@Override
	public boolean isEdgeID(long id) {
		return IDType.Edge.is(id);
	}


    public final static long getSystemEdgeLabelID(long id) {
        //assumes groupid=0 and partitionid=0
        return IDManager.IDType.RelationshipType.addPadding(id);
    }

    public final static long getSystemPropertyKeyID(long id) {
        //assumes groupid=0 and partitionid=0
        return IDManager.IDType.PropertyType.addPadding(id);
    }

}

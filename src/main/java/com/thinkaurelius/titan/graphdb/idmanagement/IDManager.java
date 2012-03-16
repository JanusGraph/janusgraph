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
			assert id>0;
			return (id<<offset()) | id();
		}
		public final boolean is(long id) {
			assert id>0;
			return (id&((1l<<offset())-1)) == id();
		}
	}

	public static final long totalNoBits = 63;
	public static final long paddingBits = 4;
	public static final long maxGroupBits = 31;
	public static final long maxPartitionBits = 31;
	public static final long edgeTypeDirectionBits = 2;
	public static final long edgeTypePaddingBits = 3;
	
	public static final long defaultPartitionBits = 20;
	public static final long defaultGroupBits = 10;
	
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

	private final long groupIDMask;
	private final long groupIDFrontMask;
	private final long groupIDDeltaOffset;
	private final long edgeTypeIDBackLength;
	private final long edgeTypeCountPartMask;
	
	private final long maxGroupID;
	private final long maxEdgeID;
	private final long maxEdgeTypeID;
	private final long maxNodeID;
	
	private final long maxPartitionID;

    private final ReferenceNodeIdentifier referenceNodeIdentifier;
	
	public IDManager(long partitionBits, long groupBits, ReferenceNodeIdentifier refnodeidentifier) {
		if (partitionBits>maxPartitionBits)
			throw new IllegalArgumentException("Partition bits can be at most " + maxPartitionBits);
		if (groupBits>maxGroupBits)
			throw new IllegalArgumentException("Group bits can be at most " + maxGroupBits);
		if (totalNoBits-partitionBits-1<paddingBits)
			throw new IllegalArgumentException("No bits left for id");
		if (totalNoBits-partitionBits-groupBits-1<paddingBits)
			throw new IllegalArgumentException("Too many group bits");
		this.partitionBits=partitionBits;
		this.groupBits=groupBits;
		maxGroupID = (1l<<groupBits)-2; //Need extra number for bounding
		maxEdgeID = (1l<<(totalNoBits- IDType.Edge.offset()))-1;
		maxEdgeTypeID = (1l<<(totalNoBits-partitionBits- IDType.RelationshipType.offset()
										 -groupBits-edgeTypeDirectionBits))-1;
		maxNodeID = (1l<<(totalNoBits-partitionBits- IDType.Node.offset()))-1;
		
		maxPartitionID= (1l<<(partitionBits))-1;
		
		partitionOffset = totalNoBits-partitionBits;
		edgeTypeIDBackLength=edgeTypePaddingBits + groupBits + edgeTypeDirectionBits;
		groupIDMask = ((1l<<groupBits)-1) << edgeTypePaddingBits;
		groupIDDeltaOffset = edgeTypePaddingFrontOffset-groupBits-edgeTypePaddingBits;
		groupIDFrontMask = groupIDMask << groupIDDeltaOffset;
		
		edgeTypeCountPartMask = (1l<<(totalNoBits-edgeTypeIDBackLength))-1;

        this.referenceNodeIdentifier=refnodeidentifier;
	}

    public IDManager(ReferenceNodeIdentifier referenceNodeIdentifier) {
        this(defaultPartitionBits,defaultGroupBits,referenceNodeIdentifier);
    }

    public IDManager(long partitionBits, long groupBits) {
        this(partitionBits,groupBits, ReferenceNodeIdentifier.noReferenceNodes);
    }

    public IDManager() {
		this(defaultPartitionBits, defaultGroupBits, ReferenceNodeIdentifier.noReferenceNodes);
	}
	
	private long addPartition(long id, long partitionID) {
		if (id<0) throw new IllegalArgumentException("ID cannot be negative: " + id);
		if (partitionID<0) throw new IllegalArgumentException("Partition ID cannot be negative: " + partitionID);
		if (partitionID>maxPartitionID)
			throw new IllegalArgumentException("Partition ID exceeds limit of: " + maxPartitionID);
		assert id<(1l<<partitionOffset) : "ID is too large for partition offset: " + id;
		return (partitionID<<partitionOffset) | id;
	}
	
	private long addGroup(long id, long groupID) {
		if (groupID<=0) throw new IllegalArgumentException("Group ID must be positive: " + groupID);
		if (groupID>maxGroupID)
			throw new IllegalArgumentException("Group ID exceeds limit of: " + maxGroupID);
		return (id<<groupBits) | groupID;
	}

    @Override
	public boolean isValidEdgeTypGroupID(long groupID) {
		return groupID>0 && groupID<=maxGroupID;
	}

	public long getEdgeID(long count) {
		if (count<0 || count>maxEdgeID) 
			throw new IllegalArgumentException("Invalid count for bound:" + maxEdgeID);
		return IDType.Edge.addPadding(count);
	}
	
	public long getRelationshipTypeID(long count, long groupid, long partition) {
		if (count<0 || count>maxEdgeTypeID) 
			throw new IllegalArgumentException("Invalid count for bound:" + maxEdgeTypeID);
        return addPartition(IDType.RelationshipType.addPadding(addGroup(count << edgeTypeDirectionBits, groupid)), partition);
	}
	
	public long getPropertyTypeID(long count, long groupid, long partition) {
		if (count<0 || count>maxEdgeTypeID) 
			throw new IllegalArgumentException("Invalid count for bound:" + maxEdgeTypeID);
        return addPartition(IDType.PropertyType.addPadding(addGroup(count << edgeTypeDirectionBits, groupid)), partition);
	}
	
	public long getNodeID(long count, long partition) {
		if (count<0 || count>maxNodeID) 
			throw new IllegalArgumentException("Invalid count for bound:" + maxNodeID);
		return addPartition(IDType.Node.addPadding(count), partition);
	}

	
	/*		    	--- normal ---										  --- Group ID in front ---
	 *  [ 0 | partitionID | count | directionBits (=0) | groupID | ID padding ] <-> [ 0 | directionBits | ID padding | groupID | partitionID | count ]
	 *  EdgeTypeIDack = directionBits (=0) | groupID | ID padding
	 */

	public final long switchEdgeTypeID(long edgetypeid, long direction) {
		assert direction<=maxDirectionID && direction>=0;
		long idpadding = (edgetypeid & edgeTypePaddingMask)<<edgeTypePaddingFrontOffset;
		long group = (edgetypeid & groupIDMask) << groupIDDeltaOffset;
		//add direction
		return (edgetypeid>>>edgeTypeIDBackLength) | (direction<<edgeTypeDirectionOffset) | idpadding | group;
	}

	public final long switchBackEdgeTypeID(long edgetypeid) {
		//first, remove direction
		long idpadding = (edgetypeid & edgeTypePaddingFrontMask)>>edgeTypePaddingFrontOffset;
		assert idpadding== IDType.PropertyType.id() || idpadding== IDType.RelationshipType.id();
		long group = (edgetypeid & groupIDFrontMask) >> groupIDDeltaOffset;
		assert (group>>edgeTypePaddingBits)>=0 && (group>>edgeTypePaddingBits)<=maxGroupID : 
			(group>>edgeTypePaddingBits) + " < "  + maxGroupID;
		
		edgetypeid = (edgetypeid & edgeTypeCountPartMask)<<edgeTypeIDBackLength;
		return edgetypeid | group | idpadding;
	}

	public long getDirectionFront(long edgetypeid) {
		return (edgetypeid & edgeTypeDirectionMask)>>>edgeTypeDirectionOffset;
	}

	public long getGroupIDFront(long edgetypeid) {
		return (edgetypeid & groupIDFrontMask) >> (groupIDDeltaOffset+edgeTypePaddingBits);
	}
	
	private long getEdgeTypePaddingFront(long edgetypeid) {
		return (edgetypeid & edgeTypePaddingFrontMask)>>edgeTypePaddingFrontOffset;
	}
	
	public boolean isPropertyTypeFront(long edgetypeid) {
		return IDType.PropertyType.is(getEdgeTypePaddingFront(edgetypeid));
	}
	
	public boolean isRelationshipTypeFront(long edgetypeid) {
		return IDType.RelationshipType.is(getEdgeTypePaddingFront(edgetypeid));
	}

    @Override
	public long[] getQueryBounds(long direction) {
		assert direction<=maxDirectionID && direction>=0;
		long res = direction;
		return new long[]{res<<edgeTypeDirectionOffset,(res+1)<<edgeTypeDirectionOffset};
	}

	private long[] getQueryBoundsEdge(long direction, long edgeTypePadding) {
		assert direction<=maxDirectionID && direction>=0;
		long res = (direction<<edgeTypePaddingBits) + edgeTypePadding;
		return new long[]{res<<edgeTypePaddingFrontOffset,(res+1)<<edgeTypePaddingFrontOffset};
	}

    @Override
    public long[] getQueryBoundsRelationship(long direction) {
		return getQueryBoundsEdge(direction, IDType.RelationshipType.id());
    }

    @Override
	public long[] getQueryBoundsProperty(long direction) {
		return getQueryBoundsEdge(direction, IDType.PropertyType.id());
	}
	
	private long[] getQueryBoundsEdge(long direction, long edgeTypePadding, long group) {
		assert direction<=maxDirectionID && direction>=0;
		long res = (direction<<edgeTypePaddingBits) + edgeTypePadding;
		assert group<=maxGroupID && group>=0;
		res = (res << groupBits) + group;
		return new long[]{res<<(edgeTypePaddingFrontOffset-groupBits),(res+1)<<(edgeTypePaddingFrontOffset-groupBits)};
	}

    @Override
	public long[] getQueryBoundsRelationship(long direction, long group) {
		return getQueryBoundsEdge(direction, IDType.RelationshipType.id(), group);
	}

    @Override
	public long[] getQueryBoundsProperty(long direction, long group) {
		return getQueryBoundsEdge(direction, IDType.PropertyType.id(), group);
	}

    @Override
	public long[] getQueryBounds(long edgetypeid, long direction) {
		long sw = switchEdgeTypeID(edgetypeid, direction);
		return new long[]{sw,sw+1};
	}
	
//	private long moveLastBits2Front(long id, long bits) {
//		assert id>0;
//		long mask = (1l<<bits)-1;
//		long end = id & mask;
//		id = id >>> bits;
//		return (end<<(totalNoBits-bits) | id);
//	}
	
	
	
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
	public boolean isReferenceNodeID(long id) {
        return referenceNodeIdentifier.isReferenceNodeID(id);
	}

	@Override
	public boolean isRelationshipTypeID(long id) {
		return IDType.RelationshipType.is(id);
	}

	@Override
	public boolean isEdgeID(long id) {
		return IDType.Edge.is(id);
	}

	
}

package com.thinkaurelius.titan.graphdb.idmanagement;

public interface IDInspector {
	
	public boolean isEdgeID(long id);
	
	public boolean isEdgeTypeID(long id);
	
	public boolean isRelationshipTypeID(long id);
	
	public boolean isPropertyTypeID(long id);
	
	public boolean isNodeID(long id);

	public long getPartitionID(long id);


    public boolean isValidEdgeTypGroupID(long groupid);


//    public long[] getQueryBounds(long direction);
//
//    public long[] getQueryBoundsRelationship(long direction);
//
//    public long[] getQueryBoundsProperty(long direction);
//
//    public long[] getQueryBoundsRelationship(long direction, long group);
//
//    public long[] getQueryBoundsProperty(long direction, long group);
//
//    public long[] getQueryBounds(long edgetypeid, long direction);

	
}

package com.thinkaurelius.titan.graphdb.idmanagement;

public interface IDInspector {

    public boolean isEdgeID(long id);

    public boolean isEdgeTypeID(long id);

    public boolean isRelationshipTypeID(long id);

    public boolean isPropertyTypeID(long id);

    public boolean isNodeID(long id);

    public long getPartitionID(long id);

    public long getGroupID(long etId);

    public boolean isValidEdgeTypGroupID(long groupid);


}

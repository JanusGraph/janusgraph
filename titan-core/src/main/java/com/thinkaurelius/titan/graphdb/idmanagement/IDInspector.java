package com.thinkaurelius.titan.graphdb.idmanagement;

/**
 * Interface for determining the type of element a particular id refers to.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IDInspector {

    public boolean isRelationID(long id);

    public boolean isTypeID(long id);

    public boolean isEdgeLabelID(long id);

    public boolean isPropertyKeyID(long id);

    public boolean isVertexID(long id);

    public long getPartitionID(long id);

}

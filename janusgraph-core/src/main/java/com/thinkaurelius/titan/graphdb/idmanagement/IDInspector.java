package com.thinkaurelius.titan.graphdb.idmanagement;

/**
 * Interface for determining the type of element a particular id refers to.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface IDInspector {

    public boolean isSchemaVertexId(long id);

    public boolean isRelationTypeId(long id);

    public boolean isEdgeLabelId(long id);

    public boolean isPropertyKeyId(long id);

    public boolean isSystemRelationTypeId(long id);

    public boolean isVertexLabelVertexId(long id);

    public boolean isGenericSchemaVertexId(long id);

    public boolean isUserVertexId(long id);

    public boolean isUnmodifiableVertex(long id);

    public boolean isPartitionedVertex(long id);

    public long getCanonicalVertexId(long partitionedVertexId);

}

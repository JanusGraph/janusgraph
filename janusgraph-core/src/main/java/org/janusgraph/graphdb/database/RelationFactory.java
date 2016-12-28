package org.janusgraph.graphdb.database;

import org.janusgraph.core.RelationType;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface RelationFactory {

    public long getVertexID();

    public void setDirection(Direction dir);

    public void setType(RelationType type);

    public void setRelationID(long relationID);

    public void setOtherVertexID(long vertexId);

    public void setValue(Object value);

    public void addProperty(RelationType type, Object value);

}

package com.thinkaurelius.titan.graphdb.database;

import com.thinkaurelius.titan.core.TitanType;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface RelationFactory {

    public long getVertexID();

    public void setDirection(Direction dir);

    public void setType(TitanType type);

    public void setRelationID(long relationID);

    public void setOtherVertexID(long vertexId);

    public void setValue(Object value);

    public void addProperty(TitanType type, Object value);

}

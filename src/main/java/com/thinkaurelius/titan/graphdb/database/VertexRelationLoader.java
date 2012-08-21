package com.thinkaurelius.titan.graphdb.database;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.tinkerpop.blueprints.Direction;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface VertexRelationLoader {

    public void loadProperty(long propertyid, TitanKey key, Object attribute);
    
    public void loadEdge(long edgeid, TitanLabel label, Direction dir, long otherVertexId);
    
    public void addRelationProperty(TitanKey key, Object attribute);

    public void addRelationEdge(TitanLabel label, long vertexId);

    public long getVertexId();

}

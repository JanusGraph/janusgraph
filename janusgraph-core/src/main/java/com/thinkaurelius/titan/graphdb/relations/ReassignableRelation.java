package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ReassignableRelation {

    public void setVertexAt(int pos, InternalVertex vertex);

}

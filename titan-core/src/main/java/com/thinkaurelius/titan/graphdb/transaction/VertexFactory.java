package com.thinkaurelius.titan.graphdb.transaction;

import com.thinkaurelius.titan.graphdb.internal.InternalVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexFactory {

    public InternalVertex getInternalVertex(long id);

}

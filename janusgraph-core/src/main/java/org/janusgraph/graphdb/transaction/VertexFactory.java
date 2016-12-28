package org.janusgraph.graphdb.transaction;

import org.janusgraph.graphdb.internal.InternalVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexFactory {

    public InternalVertex getInternalVertex(long id);

}

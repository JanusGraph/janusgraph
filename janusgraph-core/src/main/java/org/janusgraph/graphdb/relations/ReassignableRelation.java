package org.janusgraph.graphdb.relations;

import org.janusgraph.graphdb.internal.InternalVertex;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface ReassignableRelation {

    public void setVertexAt(int pos, InternalVertex vertex);

}

package org.janusgraph.core;

import org.janusgraph.core.schema.JanusGraphSchemaType;

/**
 * A vertex label is a label attached to vertices in a JanusGraph graph. This can be used to define the nature of a
 * vertex.
 * <p />
 * Internally, a vertex label is also used to specify certain characteristics of vertices that have a given label.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabel extends JanusGraphVertex, JanusGraphSchemaType {

    /**
     * Whether vertices with this label are partitioned.
     *
     * @return
     */
    public boolean isPartitioned();

    /**
     * Whether vertices with this label are static, that is, immutable beyond the transaction
     * in which they were created.
     *
     * @return
     */
    public boolean isStatic();

    //TTL


}

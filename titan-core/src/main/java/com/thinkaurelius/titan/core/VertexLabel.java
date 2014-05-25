package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.core.schema.TitanSchemaType;

/**
 * A vertex label is a label attached to vertices in a Titan graph. This can be used to define the nature of a
 * vertex.
 * <p />
 * Internally, a vertex label is also used to specify certain characteristics of vertices that have a given label.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabel extends TitanVertex, TitanSchemaType {

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

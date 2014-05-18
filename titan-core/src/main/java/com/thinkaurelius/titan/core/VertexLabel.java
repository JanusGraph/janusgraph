package com.thinkaurelius.titan.core;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabel extends TitanVertex, TitanSchemaType {

    public boolean isPartitioned();

    public boolean isStatic();

    //TTL


}

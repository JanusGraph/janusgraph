package com.thinkaurelius.titan.core;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabelMaker {

    public String getName();

    public VertexLabelMaker partition();

    public VertexLabelMaker setStatic();

    public VertexLabel make();


}

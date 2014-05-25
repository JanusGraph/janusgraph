package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexLabelMaker {

    public String getName();

    public VertexLabelMaker partition();

    public VertexLabelMaker setStatic();

    public VertexLabel make();


}

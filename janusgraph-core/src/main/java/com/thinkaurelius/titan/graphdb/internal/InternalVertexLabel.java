package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalVertexLabel extends VertexLabel {

    public boolean hasDefaultConfiguration();

    public int getTTL();


}

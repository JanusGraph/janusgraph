package org.janusgraph.graphdb.internal;

import org.janusgraph.core.VertexLabel;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalVertexLabel extends VertexLabel {

    public boolean hasDefaultConfiguration();

    public int getTTL();


}

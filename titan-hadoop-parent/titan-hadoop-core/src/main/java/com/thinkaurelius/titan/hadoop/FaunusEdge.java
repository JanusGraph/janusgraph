package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.TitanEdge;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface FaunusEdge extends FaunusRelation, TitanEdge {

    public long getVertexId(Direction dir);

}

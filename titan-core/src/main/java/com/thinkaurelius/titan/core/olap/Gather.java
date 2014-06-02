package com.thinkaurelius.titan.core.olap;

import com.thinkaurelius.titan.core.TitanEdge;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface Gather<S,M> {

    public M apply(S state, TitanEdge edge, Direction dir);

}

package com.thinkaurelius.titan.core;

import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TitanTypeIndex extends TitanSchemaElement {

    public TitanType getType();

    public Order getSortOrder();

    public TitanType[] getSortKey();

    public Direction getDirection();


}

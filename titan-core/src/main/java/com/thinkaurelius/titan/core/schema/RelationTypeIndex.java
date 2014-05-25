package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.RelationType;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface RelationTypeIndex extends TitanSchemaElement {

    public RelationType getType();

    public Order getSortOrder();

    public RelationType[] getSortKey();

    public Direction getDirection();


}

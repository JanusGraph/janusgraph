package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.TypeAttribute;
import com.tinkerpop.blueprints.Direction;

/**
 * Internal Type interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalType extends TitanType, InternalVertex {

    public boolean isHidden();

    public boolean uniqueLock(Direction direction);

    public long[] getSignature();

    public long[] getSortKey();

    public Order getSortOrder();

    public Integer getTtl();

}

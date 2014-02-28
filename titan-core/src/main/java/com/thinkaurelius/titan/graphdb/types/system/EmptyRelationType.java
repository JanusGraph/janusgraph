package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.types.IndexType;

import java.util.Collections;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class EmptyRelationType extends EmptyVertex implements InternalRelationType {

    @Override
    public boolean isHidden() {
        return true;
    }

    @Override
    public long[] getSignature() {
        return new long[0];
    }

    @Override
    public long[] getSortKey() {
        return new long[0];
    }

    @Override
    public Order getSortOrder() {
        return Order.ASC;
    }

    @Override
    public InternalRelationType getBaseType() {
        return null;
    }

    @Override
    public Iterable<InternalRelationType> getRelationIndexes() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public Iterable<IndexType> getKeyIndexes() {
        return Collections.EMPTY_LIST;
    }

}

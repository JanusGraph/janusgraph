package com.thinkaurelius.titan.graphdb.types.system;

import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.SchemaStatus;

import java.util.Collections;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class EmptyType extends EmptyVertex implements InternalType {

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
    public InternalType getBaseType() {
        return null;
    }

    @Override
    public Iterable<InternalType> getRelationIndexes() {
        return Collections.EMPTY_LIST;
    }

    @Override
    public SchemaStatus getStatus() {
        return SchemaStatus.ENABLED;
    }

    @Override
    public Iterable<IndexType> getKeyIndexes() {
        return Collections.EMPTY_LIST;
    }

}

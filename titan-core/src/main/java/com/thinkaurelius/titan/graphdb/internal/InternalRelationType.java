package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.tinkerpop.blueprints.Direction;

/**
 * Internal Type interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalRelationType extends TitanType, InternalVertex {

    public boolean isHiddenRelationType();

    public long[] getSignature();

    public long[] getSortKey();

    public Order getSortOrder();

    public Multiplicity getMultiplicity();

    public ConsistencyModifier getConsistencyModifier();


    public boolean isUnidirected(Direction dir);

    public InternalRelationType getBaseType();

    public boolean isEnabled();

    public Iterable<InternalRelationType> getRelationIndexes();


    public Iterable<IndexType> getKeyIndexes();
}

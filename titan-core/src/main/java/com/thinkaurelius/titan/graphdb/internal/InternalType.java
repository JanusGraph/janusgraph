package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.SchemaStatus;
import com.tinkerpop.blueprints.Direction;

/**
 * Internal Type interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalType extends TitanType, InternalVertex {

    public boolean isHiddenType();

    public long[] getSignature();

    public long[] getSortKey();

    public Order getSortOrder();

    public Multiplicity getMultiplicity();

    public ConsistencyModifier getConsistencyModifier();


    public boolean isUnidirected(Direction dir);

    public InternalType getBaseType();

    public Iterable<InternalType> getRelationIndexes();

    public SchemaStatus getStatus();

    public Iterable<IndexType> getKeyIndexes();
}

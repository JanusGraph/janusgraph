package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * Internal Type interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalRelationType extends RelationType, InternalVertex {

    public boolean isInvisibleType();

    public long[] getSignature();

    public long[] getSortKey();

    public Order getSortOrder();

    public Multiplicity multiplicity();

    public ConsistencyModifier getConsistencyModifier();

    public Integer getTTL();

    public boolean isUnidirected(Direction dir);

    public InternalRelationType getBaseType();

    public Iterable<InternalRelationType> getRelationIndexes();

    public SchemaStatus getStatus();

    public Iterable<IndexType> getKeyIndexes();
}

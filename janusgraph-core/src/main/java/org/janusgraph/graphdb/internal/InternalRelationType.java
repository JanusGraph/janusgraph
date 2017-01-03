package org.janusgraph.graphdb.internal;

import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.RelationType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.core.schema.SchemaStatus;
import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * Internal Type interface adding methods that should only be used by Janus
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

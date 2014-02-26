package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeSource;
import com.thinkaurelius.titan.graphdb.types.indextype.ExternalIndexTypeWrapper;
import com.thinkaurelius.titan.graphdb.types.indextype.InternalIndexTypeWrapper;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanRelationTypeVertex extends TitanTypeVertex implements InternalRelationType {

    public TitanRelationTypeVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

    //####### IDENTICAL TO TitanRelationTypeReference

    @Override
    public long[] getSortKey() {
        return getDefinition().getValue(TypeDefinitionCategory.SORT_KEY, long[].class);
    }

    @Override
    public Order getSortOrder() {
        return getDefinition().getValue(TypeDefinitionCategory.SORT_ORDER, Order.class);
    }

    @Override
    public long[] getSignature() {
        return getDefinition().getValue(TypeDefinitionCategory.SIGNATURE, long[].class);
    }

    @Override
    public boolean isHiddenRelationType() {
        return getDefinition().getValue(TypeDefinitionCategory.HIDDEN, Boolean.class);
    }

    @Override
    public Multiplicity getMultiplicity() {
        return getDefinition().getValue(TypeDefinitionCategory.MULTIPLICITY, Multiplicity.class);
    }

    private ConsistencyModifier consistency = null;

    public ConsistencyModifier getConsistencyModifier() {
        if (consistency==null) {
            Entry entry = Iterables.getOnlyElement(getRelated(TypeDefinitionCategory.CONSISTENCY_MODIFIER, Direction.OUT),null);
            if (entry==null) consistency=ConsistencyModifier.DEFAULT;
            else consistency=entry.getSchemaType().getDefinition().getValue(TypeDefinitionCategory.CONSISTENCY_LEVEL,ConsistencyModifier.class);
        }
        return consistency;
    }

    public InternalRelationType getBaseType() {
        Entry entry = Iterables.getOnlyElement(getRelated(TypeDefinitionCategory.RELATION_INDEX,Direction.IN),null);
        if (entry==null) return null;
        assert entry.getSchemaType() instanceof InternalRelationType;
        return (InternalRelationType)entry.getSchemaType();
    }

    public Iterable<InternalRelationType> getRelationIndexes() {
        return Iterables.concat(ImmutableList.of(this),Iterables.transform(getRelated(TypeDefinitionCategory.RELATION_INDEX,Direction.OUT),new Function<Entry, InternalRelationType>() {
            @Nullable
            @Override
            public InternalRelationType apply(@Nullable Entry entry) {
                assert entry.getSchemaType() instanceof InternalRelationType;
                return (InternalRelationType)entry.getSchemaType();
            }
        }));
    }

    private List<IndexType> indexes = null;

    public Iterable<IndexType> getKeyIndexes() {
        if (indexes==null) {
            ImmutableList.Builder<IndexType> b = ImmutableList.builder();
            for (Entry entry : getRelated(TypeDefinitionCategory.INDEX_FIELD,Direction.IN)) {
                TypeSource index = entry.getSchemaType();
                if (index.getDefinition().getValue(TypeDefinitionCategory.IS_MAPPING,Boolean.class)) {
                    b.add(new InternalIndexTypeWrapper(index));
                } else {
                    b.add(new ExternalIndexTypeWrapper(index));
                }
            }
            indexes = b.build();
        }
        assert indexes!=null;
        return indexes;
    }

}

package com.thinkaurelius.titan.graphdb.types.vertices;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.SchemaSource;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeUtil;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class TitanTypeVertex extends TitanSchemaVertex implements InternalType {

    public TitanTypeVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
    }

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
    public boolean isHiddenType() {
        return getDefinition().getValue(TypeDefinitionCategory.HIDDEN, Boolean.class);
    }

    @Override
    public Multiplicity getMultiplicity() {
        return getDefinition().getValue(TypeDefinitionCategory.MULTIPLICITY, Multiplicity.class);
    }

    private ConsistencyModifier consistency = null;

    public ConsistencyModifier getConsistencyModifier() {
        if (consistency==null) {
            consistency = TypeUtil.getConsistencyModifier(this);
        }
        return consistency;
    }

    public InternalType getBaseType() {
        Entry entry = Iterables.getOnlyElement(getRelated(TypeDefinitionCategory.RELATIONTYPE_INDEX,Direction.IN),null);
        if (entry==null) return null;
        assert entry.getSchemaType() instanceof InternalType;
        return (InternalType)entry.getSchemaType();
    }

    public Iterable<InternalType> getRelationIndexes() {
        return Iterables.concat(ImmutableList.of(this),Iterables.transform(getRelated(TypeDefinitionCategory.RELATIONTYPE_INDEX,Direction.OUT),new Function<Entry, InternalType>() {
            @Nullable
            @Override
            public InternalType apply(@Nullable Entry entry) {
                assert entry.getSchemaType() instanceof InternalType;
                return (InternalType)entry.getSchemaType();
            }
        }));
    }

    private List<IndexType> indexes = null;

    public Iterable<IndexType> getKeyIndexes() {
        if (indexes==null) {
            ImmutableList.Builder<IndexType> b = ImmutableList.builder();
            for (Entry entry : getRelated(TypeDefinitionCategory.INDEX_FIELD,Direction.IN)) {
                SchemaSource index = entry.getSchemaType();
                b.add(index.asIndexType());
            }
            indexes = b.build();
        }
        assert indexes!=null;
        return indexes;
    }

}

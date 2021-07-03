// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.types.vertices;

import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeUtil;
import org.janusgraph.graphdb.util.CollectionsUtil;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class RelationTypeVertex extends JanusGraphSchemaVertex implements InternalRelationType {

    private ConsistencyModifier consistency = null;
    private Integer ttl = null;
    private List<IndexType> indexes = null;

    public RelationTypeVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
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
    public boolean isInvisibleType() {
        return getDefinition().getValue(TypeDefinitionCategory.INVISIBLE, Boolean.class);
    }

    @Override
    public Multiplicity multiplicity() {
        return getDefinition().getValue(TypeDefinitionCategory.MULTIPLICITY, Multiplicity.class);
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        if (consistency==null) {
            consistency = TypeUtil.getConsistencyModifier(this);
        }
        return consistency;
    }

    @Override
    public Integer getTTL() {
        if (null == ttl) {
            ttl = TypeUtil.getTTL(this);
        }
        return ttl;
    }

    @Override
    public InternalRelationType getBaseType() {
        Entry entry = Iterables.getOnlyElement(getRelated(TypeDefinitionCategory.RELATIONTYPE_INDEX,Direction.IN),null);
        if (entry==null) return null;
        assert entry.getSchemaType() instanceof InternalRelationType;
        return (InternalRelationType)entry.getSchemaType();
    }

    @Override
    public Iterable<InternalRelationType> getRelationIndexes() {
        return Stream.concat(
            Stream.of(this),
            getRelated(TypeDefinitionCategory.RELATIONTYPE_INDEX, Direction.OUT).stream()
                .map(entry -> {
                    assert entry.getSchemaType() instanceof InternalRelationType;
                    return (InternalRelationType) entry.getSchemaType();
                }))::iterator;
    }

    @Override
    public Iterable<IndexType> getKeyIndexes() {
        List<IndexType> result = indexes;
        if (result==null) {
            result = Collections.unmodifiableList(
                CollectionsUtil.toArrayList(
                    getRelated(TypeDefinitionCategory.INDEX_FIELD,Direction.IN),
                    entry -> entry.getSchemaType().asIndexType()
                )
            );
            indexes=result;
        }
        assert result!=null;
        return result;
    }

    @Override
    public void resetCache() {
        super.resetCache();
        indexes=null;
    }
}

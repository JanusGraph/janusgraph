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

package org.janusgraph.graphdb.relations;

import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.transaction.RelationConstructor;
import org.janusgraph.graphdb.types.system.ImplicitKey;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class CacheEdge extends AbstractEdge {

    public CacheEdge(long id, EdgeLabel label, InternalVertex start, InternalVertex end, Entry data) {
        super(id, label, start.it(), end.it());
        assert data != null;

        this.data = data;
    }

    public Direction getVertexCentricDirection() {
        final RelationCache cache = data.getCache();
        Preconditions.checkNotNull(cache, "Cache is null");
        return cache.direction;
    }

    //############## Similar code as CacheProperty but be careful when copying #############################

    private final Entry data;

    @Override
    public InternalRelation it() {
        InternalRelation it = null;
        InternalVertex startVertex = getVertex(0);

        if (startVertex.hasAddedRelations() && startVertex.hasRemovedRelations()) {
            //Test whether this relation has been replaced
            final long id = super.longId();
            final Iterable<InternalRelation> added = startVertex.getAddedRelations(
                internalRelation -> (internalRelation instanceof StandardEdge) && ((StandardEdge) internalRelation).getPreviousID() == id);
            assert Iterables.size(added) <= 1 || (isLoop() && Iterables.size(added) == 2);
            it = Iterables.getFirst(added, null);
        }

        if (it != null)
            return it;

        return super.it();
    }

    private void copyProperties(InternalRelation to) {
        for (LongObjectCursor<Object> entry : getPropertyMap()) {
            PropertyKey type = tx().getExistingPropertyKey(entry.key);
            if (!(type instanceof ImplicitKey))
                to.setPropertyDirect(type, entry.value);
        }
    }

    private synchronized InternalRelation update() {
        StandardEdge copy = new StandardEdge(super.longId(), edgeLabel(), getVertex(0), getVertex(1), ElementLifeCycle.Loaded);
        copyProperties(copy);
        copy.remove();

        Long id = type.getConsistencyModifier() != ConsistencyModifier.FORK ? longId() : null;
        StandardEdge u = (StandardEdge) tx().addEdge(id, getVertex(0), getVertex(1), edgeLabel());
        u.setPreviousID(longId());
        copyProperties(u);
        return u;
    }

    private RelationCache getPropertyMap() {
        RelationCache map = data.getCache();
        if (map == null || !map.hasProperties()) {
            map = RelationConstructor.readRelationCache(data, tx());
        }
        return map;
    }

    @Override
    public <O> O getValueDirect(PropertyKey key) {
        return getPropertyMap().get(key.longId());
    }

    @Override
    public Iterable<PropertyKey> getPropertyKeysDirect() {
        RelationCache map = getPropertyMap();
        List<PropertyKey> types = new ArrayList<>(map.numProperties());

        for (LongObjectCursor<Object> entry : map) {
            types.add(tx().getExistingPropertyKey(entry.key));
        }

        return types;
    }

    @Override
    public void setPropertyDirect(PropertyKey key, Object value) {
        update().setPropertyDirect(key, value);
    }

    @Override
    public <O> O removePropertyDirect(PropertyKey key) {
        return update().removePropertyDirect(key);
    }

    @Override
    public byte getLifeCycle() {
        InternalVertex startVertex = getVertex(0);
        return ((startVertex.hasRemovedRelations() || startVertex.isRemoved()) && tx().isRemovedRelation(super.longId()))
                ? ElementLifeCycle.Removed : ElementLifeCycle.Loaded;
    }

    @Override
    public void remove() {
        if (!isRemoved()) {
            tx().removeRelation(this);
        }// else throw InvalidElementException.removedException(this);
    }

}

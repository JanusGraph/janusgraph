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

package org.janusgraph.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.relations.StandardEdge;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.addedrelations.AddedRelationsContainer;
import org.janusgraph.graphdb.transaction.addedrelations.ConcurrentAddedRelations;
import org.janusgraph.graphdb.transaction.addedrelations.SimpleAddedRelations;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.util.datastructures.Retriever;

import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardVertex extends AbstractVertex {

    private final Object lifecycleMutex = new Object();
    private volatile byte lifecycle;
    private VertexLabel cachedVertexLabel;
    private volatile AddedRelationsContainer addedRelations = AddedRelationsContainer.EMPTY;

    public StandardVertex(final StandardJanusGraphTx tx, final Object id, byte lifecycle) {
        super(tx, id);
        this.lifecycle = lifecycle;
    }

    public final void updateLifeCycle(ElementLifeCycle.Event event) {
        synchronized (lifecycleMutex) {
            this.lifecycle = ElementLifeCycle.update(lifecycle, event);
        }
    }

    @Override
    public void removeRelation(InternalRelation r) {
        if (r.isNew()) {
            addedRelations.remove(r);
        } else if (r.isLoaded()) updateLifeCycle(ElementLifeCycle.Event.REMOVED_RELATION);
        else throw new IllegalArgumentException("Unexpected relation status: " + r.isRemoved());
    }

    @Override
    public boolean addRelation(InternalRelation r) {
        Preconditions.checkArgument(r.isNew(), "Unexpected relation status: r is not new");
        if (addedRelations == AddedRelationsContainer.EMPTY) {
            if (tx().getConfiguration().isSingleThreaded()) {
                addedRelations = new SimpleAddedRelations(true);
            } else {
                synchronized (this) {
                    if (addedRelations == AddedRelationsContainer.EMPTY)
                        addedRelations = new ConcurrentAddedRelations(true);
                }
            }
        }
        if (addedRelations.add(r)) {
            cacheLabelVertex(r);
            updateLifeCycle(ElementLifeCycle.Event.ADDED_RELATION);
            return true;
        } else return false;
    }

    /**
     * Returns duplicated properties for the given key and value.
     * If {@link org.janusgraph.core.Cardinality} is Single, it only matches by key name
     * If {@link org.janusgraph.core.Cardinality} is Set, it matches by key name and key value
     */
    @Override
    public Iterable<InternalRelation> getDuplicatedAddedRelation(PropertyKey key, Object value) {
        switch (key.cardinality()) {
            case SINGLE:
                return addedRelations.getViewOfProperties(key.name());
            case SET:
                return  addedRelations.getViewOfProperties(key.name(), value);
            default:
                throw new AssertionError("Unsupported cardinality for this operation");
        }
    }

    @Override
    public Iterable<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        return addedRelations.getView(query);
    }

    @Override
    public Iterable<InternalRelation> findPreviousRelation(long id) {
        return addedRelations.getViewOfPreviousRelations(id);
    }

    @Override
    public Iterable<InternalRelation> findAddedProperty(Predicate<InternalRelation> query) {
        return addedRelations.getViewOfProperties(query);
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        return (isNew()) ? EntryList.EMPTY_LIST : lookup.get(query);
    }

    @Override
    public VertexLabel vertexLabel() {
        if (this.cachedVertexLabel == null) {
            cachedVertexLabel = super.vertexLabel();
        }
        return cachedVertexLabel;
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        if (isNew()) {
            return addedRelations.getViewOfProperties(keys)
                .stream()
                .filter(i -> (keys.length != 0 || !i.isInvisible()) && ((JanusGraphVertexProperty) i).value() != null)
                .map(i -> (VertexProperty<V>) i)
                .iterator();
        } else {
            return super.properties(keys);
        }
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return false;
    }

    @Override
    public boolean hasRemovedRelations() {
        return ElementLifeCycle.hasRemovedRelations(lifecycle);
    }

    @Override
    public boolean hasAddedRelations() {
        return ElementLifeCycle.hasAddedRelations(lifecycle);
    }

    @Override
    public synchronized void remove() {
        super.remove();
        ((StandardVertex) it()).updateLifeCycle(ElementLifeCycle.Event.REMOVED);
    }

    @Override
    public synchronized void remove(Iterable<JanusGraphRelation> loadedRelations) {
        super.remove(loadedRelations);
        ((StandardVertex) it()).updateLifeCycle(ElementLifeCycle.Event.REMOVED);
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }

    private boolean cacheLabelVertex(InternalRelation relation) {
        if (relation.getType().equals(BaseLabel.VertexLabelEdge)) {
            cachedVertexLabel = (VertexLabel) ((StandardEdge) relation).vertex(Direction.IN);
            return true;
        } else {
            return false;
        }
    }
}

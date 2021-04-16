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
import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.ElementLifeCycle;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.util.ElementHelper;
import org.janusgraph.util.datastructures.Retriever;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PreloadedVertex extends CacheVertex {

    public static final Retriever<SliceQuery, EntryList> EMPTY_RETRIEVER = input -> EntryList.EMPTY_LIST;

    private PropertyMixing mixin = NO_MIXIN;
    private AccessCheck accessCheck = DEFAULT_CHECK;

    public PreloadedVertex(StandardJanusGraphTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        assert lifecycle == ElementLifeCycle.Loaded : "Invalid lifecycle encountered: " + lifecycle;
    }

    public void setPropertyMixing(PropertyMixing mixin) {
        Preconditions.checkNotNull(mixin);
        Preconditions.checkArgument(this.mixin == NO_MIXIN, "A property mixing has already been set");
        this.mixin = mixin;
    }

    public void setAccessCheck(final AccessCheck accessCheck) {
        this.accessCheck = Preconditions.checkNotNull(accessCheck);
    }

    @Override
    public void addToQueryCache(final SliceQuery query, final EntryList entries) {
        super.addToQueryCache(query, entries);
    }

    public EntryList getFromCache(final SliceQuery query) {
        return queryCache.get(query);
    }

    @Override
    public Iterable<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
        return Collections.EMPTY_LIST;
    }

    @Override
    public VertexCentricQueryBuilder query() {
        if (super.getQueryCacheSize() > 0) return super.query().queryOnlyGivenVertex();
        else throw GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated();
    }

    @Override
    public boolean hasLoadedRelations(SliceQuery query) {
        return true;
    }

    @Override
    public boolean hasRemovedRelations() {
        return false;
    }

    @Override
    public boolean hasAddedRelations() {
        return false;
    }

    @Override
    public EntryList loadRelations(SliceQuery query, Retriever<SliceQuery, EntryList> lookup) {
        return super.loadRelations(query, accessCheck.retrieveSliceQuery());
    }

    @Override
    public <V> JanusGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        accessCheck.accessSetProperty();
        JanusGraphVertexProperty<V> p = mixin.property(cardinality, key, value);
        ElementHelper.attachProperties(p, keyValues);
        return p;
    }

    public <V> JanusGraphVertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        return property(VertexProperty.Cardinality.single, key, value, keyValues);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        accessCheck.accessProperties();
        if (mixin == NO_MIXIN) return super.properties(keys);
        if (keys != null && keys.length > 0) {
            int count = 0;
            for (String key : keys) if (mixin.supports(key)) count++;
            if (count == 0 || !mixin.properties(keys).hasNext()) return super.properties(keys);
            else if (count == keys.length) return mixin.properties(keys);
        }
        return Iterators.concat(super.properties(keys), mixin.properties(keys));
    }

    @Override
    public JanusGraphEdge addEdge(String s, Vertex vertex, Object... keyValues) {
        throw GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated();
    }

    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        accessCheck.accessEdges();
        return super.edges(direction,edgeLabels);
    }

    @Override
    public void remove() {

    }

    @Override
    public void removeRelation(InternalRelation e) {
        throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
    }

    @Override
    public boolean addRelation(InternalRelation e) {
        throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
    }

    public interface AccessCheck {

        void accessEdges();

        void accessProperties();

        void accessSetProperty();

        Retriever<SliceQuery, EntryList> retrieveSliceQuery();

    }

    public static final AccessCheck DEFAULT_CHECK = new AccessCheck() {
        @Override
        public final void accessEdges() {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
        }

        @Override
        public final void accessProperties() {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
        }

        @Override
        public void accessSetProperty() {
            throw GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated();
        }

        @Override
        public Retriever<SliceQuery, EntryList> retrieveSliceQuery() {
            return EMPTY_RETRIEVER;
        }
    };

    public static final AccessCheck CLOSEDSTAR_CHECK = new AccessCheck() {
        @Override
        public final void accessEdges() { }

        @Override
        public final void accessProperties() { }

        @Override
        public void accessSetProperty() { }

        @Override
        public Retriever<SliceQuery, EntryList> retrieveSliceQuery() {
            return EXCEPTION_RETRIEVER;
        }

        private final Retriever<SliceQuery,EntryList> EXCEPTION_RETRIEVER = input -> {
            throw new UnsupportedOperationException("Cannot access data that hasn't been preloaded.");
        };
    };

    public static final AccessCheck OPENSTAR_CHECK = new AccessCheck() {
        @Override
        public final void accessEdges() { }

        @Override
        public final void accessProperties() { }

        @Override
        public void accessSetProperty() { }

        @Override
        public Retriever<SliceQuery, EntryList> retrieveSliceQuery() {
            return EMPTY_RETRIEVER;
        }
    };


    public interface PropertyMixing {

        <V> Iterator<VertexProperty<V>> properties(String... keys);

        boolean supports(String key);

        <V> JanusGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value);

    }

    private static final PropertyMixing NO_MIXIN = new PropertyMixing() {
        @Override
        public <V> Iterator<VertexProperty<V>> properties(String... keys) {
            return Collections.emptyIterator();
        }

        @Override
        public boolean supports(String key) {
            return false;
        }

        @Override
        public <V> JanusGraphVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value) {
            throw new UnsupportedOperationException("Provided key is not supported: " + key);
        }
    };

}

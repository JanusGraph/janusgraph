package com.thinkaurelius.titan.graphdb.vertices;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanVertexProperty;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.util.ElementHelper;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PreloadedVertex extends CacheVertex {

    public static final Retriever<SliceQuery, EntryList> EMPTY_RETRIEVER = new Retriever<SliceQuery, EntryList>() {
        @Override
        public EntryList get(SliceQuery input) {
            return EntryList.EMPTY_LIST;
        }
    };

    private PropertyMixing mixin = NO_MIXIN;
    private AccessCheck accessCheck = DEFAULT_CHECK;

    public PreloadedVertex(StandardTitanTx tx, long id, byte lifecycle) {
        super(tx, id, lifecycle);
        assert lifecycle == ElementLifeCycle.Loaded : "Invalid lifecycle encountered: " + lifecycle;
    }

    public void setPropertyMixing(PropertyMixing mixin) {
        Preconditions.checkNotNull(mixin);
        Preconditions.checkArgument(this.mixin == NO_MIXIN, "A property mixing has already been set");
        this.mixin = mixin;
    }

    public void setAccessCheck(final AccessCheck accessCheck) {
        Preconditions.checkArgument(accessCheck!=null);
        this.accessCheck=accessCheck;
    }

    @Override
    public void addToQueryCache(final SliceQuery query, final EntryList entries) {
        super.addToQueryCache(query, entries);
    }

    public EntryList getFromCache(final SliceQuery query) {
        return queryCache.get(query);
    }

    @Override
    public List<InternalRelation> getAddedRelations(Predicate<InternalRelation> query) {
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
    public <V> TitanVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        accessCheck.accessSetProperty();
        TitanVertexProperty<V> p = mixin.property(cardinality, key, value);
        ElementHelper.attachProperties(p, keyValues);
        return p;
    }

    public <V> TitanVertexProperty<V> property(final String key, final V value, final Object... keyValues) {
        return property(VertexProperty.Cardinality.single, key, value, keyValues);
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        accessCheck.accessProperties();
        if (mixin == NO_MIXIN) return super.properties(keys);
        if (keys != null && keys.length > 0) {
            int count = 0;
            for (int i = 0; i < keys.length; i++) if (mixin.supports(keys[i])) count++;
            if (count == 0) return super.properties(keys);
            else if (count == keys.length) return mixin.properties(keys);
        }
        return (Iterator) com.google.common.collect.Iterators.concat(super.properties(keys), mixin.properties(keys));
    }

    @Override
    public TitanEdge addEdge(String s, Vertex vertex, Object... keyValues) {
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

        public void accessEdges();

        public void accessProperties();

        public void accessSetProperty();

        public Retriever<SliceQuery, EntryList> retrieveSliceQuery();

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
        public final void accessEdges() {
            return; //Allowed
        }

        @Override
        public final void accessProperties() {
            return; //Allowed
        }

        @Override
        public void accessSetProperty() {
            return; //Allowed
        }

        @Override
        public Retriever<SliceQuery, EntryList> retrieveSliceQuery() {
            return EXCEPTION_RETRIEVER;
        }

        private final Retriever<SliceQuery,EntryList> EXCEPTION_RETRIEVER = new Retriever<SliceQuery, EntryList>() {
            @Override
            public EntryList get(SliceQuery input) {
                throw new UnsupportedOperationException("Cannot access data that hasn't been preloaded.");
            }
        };
    };

    public static final AccessCheck OPENSTAR_CHECK = new AccessCheck() {
        @Override
        public final void accessEdges() {
            return; //Allowed
        }

        @Override
        public final void accessProperties() {
            return; //Allowed
        }

        @Override
        public void accessSetProperty() {
            return; //Allowed
        }

        @Override
        public Retriever<SliceQuery, EntryList> retrieveSliceQuery() {
            return EMPTY_RETRIEVER;
        }
    };


    public interface PropertyMixing {

        public <V> Iterator<VertexProperty<V>> properties(String... keys);

        public boolean supports(String key);

        public <V> TitanVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value);

    }

    private static PropertyMixing NO_MIXIN = new PropertyMixing() {
        @Override
        public <V> Iterator<VertexProperty<V>> properties(String... keys) {
            return Collections.emptyIterator();
        }

        @Override
        public boolean supports(String key) {
            return false;
        }

        @Override
        public <V> TitanVertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value) {
            throw new UnsupportedOperationException("Provided key is not supported: " + key);
        }
    };

}

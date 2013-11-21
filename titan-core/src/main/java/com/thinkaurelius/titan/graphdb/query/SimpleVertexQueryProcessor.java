package com.thinkaurelius.titan.graphdb.query;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.RelationQueryCache;
import com.thinkaurelius.titan.graphdb.internal.InternalType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationType;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * This is a premature optimization of QueryProcessor for a special type of VertexQuery under certain
 * restrictive conditions.
 *
 * This Iterable is not thread-safe.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleVertexQueryProcessor implements Iterable<Entry> {

    private static final Logger log = LoggerFactory.getLogger(SimpleVertexQueryProcessor.class);
    private static final int INITIAL_QUERY_LIMIT = 10000;

    private final InternalVertex vertex;
    private final StandardTitanTx tx;
    private final EdgeSerializer edgeSerializer;

    private SliceQuery sliceQuery;
    private int limit = Integer.MAX_VALUE;

    private Direction filterDirection;
    private TitanKey key;
    private boolean filterHiddenProperties;

    private SimpleVertexQueryProcessor(InternalVertex vertex) {
        Preconditions.checkArgument(vertex.isLoaded(),"SimpleVertexQuery only applies to unmodified vertices");
        this.vertex = vertex;
        this.tx = vertex.tx();
        this.edgeSerializer = this.tx.getEdgeSerializer();
    }

    public SimpleVertexQueryProcessor(InternalVertex vertex, TitanKey key) {
        this(vertex);
        assert key==null || !((InternalType)key).isHidden();
        assert key==null || !((InternalType)key).isStatic(Direction.OUT);
        RelationQueryCache cache = tx.getGraph().getRelationCache();
        filterHiddenProperties = key==null;
        if (key==null || tx.getConfiguration().hasPropertyPrefetching()) {
            this.key = key;
            sliceQuery = cache.getQuery(RelationType.PROPERTY);
        } else {
            sliceQuery = cache.getQuery((InternalType)key,Direction.OUT);
        }
    }

    public SimpleVertexQueryProcessor(InternalVertex vertex, Direction dir, TitanLabel label,
                                      EdgeSerializer.TypedInterval[] sortKeyConstraints, int limit) {
        this(vertex);
        Preconditions.checkNotNull(dir);
        RelationQueryCache cache = tx.getGraph().getRelationCache();
        if (label==null) {
            assert sortKeyConstraints==null;
            sliceQuery = cache.getQuery(RelationType.EDGE);
            filterDirection = dir==Direction.BOTH?null:dir;
        } else {
            if (AbstractVertexCentricQueryBuilder.hasSortKeyConstraints(sortKeyConstraints)) {
                sliceQuery = edgeSerializer.getQuery((InternalType)label,dir,sortKeyConstraints,null);
            } else {
                sliceQuery = cache.getQuery((InternalType)label,dir);
            }
            filterDirection = null;
        }

        this.limit = limit;
        if (limit!=Query.NO_LIMIT) {
            sliceQuery = sliceQuery.updateLimit(limit);
        }
    }


    @Override
    public Iterator<Entry> iterator() {
        Iterator<Entry> iter;
        if (sliceQuery.hasLimit() && sliceQuery.getLimit()!=this.limit) {
            iter = new LimitAdjustingIterator();
        } else {
            iter = getBasicIterator();
        }
        if (filterDirection!=null) {
            assert filterDirection != Direction.BOTH;
            iter = Iterators.filter(iter, new Predicate<Entry>() {
                @Override
                public boolean apply(@Nullable Entry entry) {
                    return edgeSerializer.parseDirection(entry) == filterDirection;
                }
            });
        }
        return iter;
    }

    public Iterable<TitanRelation> relations() {
        return Iterables.transform(this,new Function<Entry, TitanRelation>() {
            @Nullable
            @Override
            public TitanRelation apply(@Nullable Entry entry) {
                return edgeSerializer.readRelation(vertex, entry);
            }
        });
    }

    public Iterable<TitanEdge> titanEdges() {
        return (Iterable) relations();
    }

    public Iterable<TitanProperty> properties() {
        Iterable<TitanProperty> result = (Iterable) relations();
        if (filterHiddenProperties || key!=null) {
            result = Iterables.filter(result, new Predicate<TitanProperty>() {
                @Override
                public boolean apply(@Nullable TitanProperty titanProperty) {
                    return (!filterHiddenProperties || !((InternalType)titanProperty.getPropertyKey()).isHidden())
                            && (key==null || titanProperty.getPropertyKey().equals(key));
                }
            });
        }
        return result;
    }

    public Iterable<Edge> edges() {
        return (Iterable) relations();
    }

    public Iterable<Vertex> vertices() {
        return Iterables.transform(this,new Function<Entry, Vertex>() {
            @Nullable
            @Override
            public Vertex apply(@Nullable Entry entry) {
                return tx.getExistingVertex(edgeSerializer.readRelation(vertex.getID(),entry,true,tx).getOtherVertexId());
            }
        });
    }

    public VertexList vertexIds() {
        AbstractLongList list = new LongArrayList();
        for (Long id : Iterables.transform(this,new Function<Entry, Long>() {
            @Nullable
            @Override
            public Long apply(@Nullable Entry entry) {
                return edgeSerializer.readRelation(vertex.getID(),entry,true,tx).getOtherVertexId();
            }
        })) {
            list.add(id);
        }
        return new VertexLongList(tx,list);
    }

    private Iterator<Entry> getBasicIterator() {
        return vertex.loadRelations(sliceQuery, new Retriever<SliceQuery, List<Entry>>() {
            @Override
            public List<Entry> get(SliceQuery query) {
                return tx.getGraph().edgeQuery(vertex.getID(), query, tx.getTxHandle());
            }
        }).iterator();
    }


    private final class LimitAdjustingIterator extends com.thinkaurelius.titan.graphdb.query.LimitAdjustingIterator<Entry> {

        private LimitAdjustingIterator() {
            super(limit,sliceQuery.getLimit());
        }

        @Override
        public Iterator<Entry> getNewIterator(int newLimit) {
            if (newLimit>sliceQuery.getLimit())
                sliceQuery = sliceQuery.updateLimit(newLimit);
            return getBasicIterator();
        }
    }



}

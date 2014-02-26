package com.thinkaurelius.titan.graphdb.query.vertex;

import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.RelationQueryCache;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
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

    private final VertexCentricQuery query;
    private final StandardTitanTx tx;
    private final EdgeSerializer edgeSerializer;
    private final InternalVertex vertex;

    private SliceQuery sliceQuery;

    public SimpleVertexQueryProcessor(VertexCentricQuery query, StandardTitanTx tx) {
        Preconditions.checkArgument(query.isSimple());
        this.query=query;
        this.tx=tx;
        this.sliceQuery=query.getSubQuery(0).getBackendQuery();
        this.vertex=query.getVertex();
        this.edgeSerializer=tx.getEdgeSerializer();
    }

    @Override
    public Iterator<Entry> iterator() {
        Iterator<Entry> iter;
        if (sliceQuery.hasLimit() && sliceQuery.getLimit()!=query.getLimit()) {
            iter = new LimitAdjustingIterator();
        } else {
            iter = getBasicIterator();
        }
        return iter;
    }

    public Iterable<TitanRelation> relations() {
        return RelationConstructor.readRelation(vertex, this, tx);
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
        return vertex.loadRelations(sliceQuery, new Retriever<SliceQuery, EntryList>() {
            @Override
            public EntryList get(SliceQuery query) {
                return tx.getGraph().edgeQuery(vertex.getID(), query, tx.getTxHandle());
            }
        }).iterator();
    }


    private final class LimitAdjustingIterator extends com.thinkaurelius.titan.graphdb.query.LimitAdjustingIterator<Entry> {

        private LimitAdjustingIterator() {
            super(query.getLimit(),sliceQuery.getLimit());
        }

        @Override
        public Iterator<Entry> getNewIterator(int newLimit) {
            if (newLimit>sliceQuery.getLimit())
                sliceQuery = sliceQuery.updateLimit(newLimit);
            return getBasicIterator();
        }
    }



}

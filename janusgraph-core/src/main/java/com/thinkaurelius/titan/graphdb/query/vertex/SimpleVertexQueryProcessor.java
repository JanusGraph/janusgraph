package com.thinkaurelius.titan.graphdb.query.vertex;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.BackendQueryHolder;
import com.thinkaurelius.titan.graphdb.query.profile.QueryProfiler;
import com.thinkaurelius.titan.graphdb.transaction.RelationConstructor;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.util.datastructures.Retriever;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

/**
 * This is an optimization of specifically for {@link VertexCentricQuery} that addresses the special but
 * common case that the query is simple (i.e. comprised of only one sub-query and that query is fitted, i.e. does not require
 * in memory filtering). Under these assumptions we can remove a lot of the steps in {@link com.thinkaurelius.titan.graphdb.query.QueryProcessor}:
 * merging of result sets, in-memory filtering and the object instantiation required for in-memory filtering.
 * </p>
 * With those complexities removed, the query processor can be much simpler which makes it a lot faster and less
 * memory intense.
 * </p>
 * IMPORTANT: This Iterable is not thread-safe.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleVertexQueryProcessor implements Iterable<Entry> {

    private static final Logger log = LoggerFactory.getLogger(SimpleVertexQueryProcessor.class);

    private final VertexCentricQuery query;
    private final StandardTitanTx tx;
    private final EdgeSerializer edgeSerializer;
    private final InternalVertex vertex;
    private final QueryProfiler profiler;

    private SliceQuery sliceQuery;

    public SimpleVertexQueryProcessor(VertexCentricQuery query, StandardTitanTx tx) {
        Preconditions.checkArgument(query.isSimple());
        this.query=query;
        this.tx=tx;
        BackendQueryHolder<SliceQuery> bqh = query.getSubQuery(0);
        this.sliceQuery=bqh.getBackendQuery();
        this.profiler=bqh.getProfiler();
        this.vertex=query.getVertex();
        this.edgeSerializer=tx.getEdgeSerializer();
    }

    @Override
    public Iterator<Entry> iterator() {
        Iterator<Entry> iter;
        //If there is a limit we need to wrap the basic iterator in a LimitAdjustingIterator which ensures the right number
        //of elements is returned. Otherwise we just return the basic iterator.
        if (sliceQuery.hasLimit() && sliceQuery.getLimit()!=query.getLimit()) {
            iter = new LimitAdjustingIterator();
        } else {
            iter = getBasicIterator();
        }
        return iter;
    }

    /**
     * Converts the entries from this query result into actual {@link TitanRelation}.
     *
     * @return
     */
    public Iterable<TitanRelation> relations() {
        return RelationConstructor.readRelation(vertex, this, tx);
    }

    /**
     * Returns the list of adjacent vertex ids for this query. By reading those ids
     * from the entries directly (without creating objects) we get much better performance.
     *
     * @return
     */
    public VertexList vertexIds() {
        LongArrayList list = new LongArrayList();
        long previousId = 0;
        for (Long id : Iterables.transform(this,new Function<Entry, Long>() {
            @Nullable
            @Override
            public Long apply(@Nullable Entry entry) {
                return edgeSerializer.readRelation(entry,true,tx).getOtherVertexId();
            }
        })) {
            list.add(id);
            if (id>=previousId && previousId>=0) previousId=id;
            else previousId=-1;
        }
        return new VertexLongList(tx,list,previousId>=0);
    }

    /**
     * Executes the query by executing its on {@link SliceQuery} sub-query.
     *
     * @return
     */
    private Iterator<Entry> getBasicIterator() {
        EntryList result = vertex.loadRelations(sliceQuery, new Retriever<SliceQuery, EntryList>() {
            @Override
            public EntryList get(SliceQuery query) {
                return QueryProfiler.profile(profiler,query, q -> tx.getGraph().edgeQuery(vertex.longId(), q, tx.getTxHandle()));
            }
        });
        return result.iterator();
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

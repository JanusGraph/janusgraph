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

package org.janusgraph.graphdb.query.vertex;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.VertexList;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.transaction.RelationConstructor;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * This is an optimization of specifically for {@link VertexCentricQuery} that addresses the special but
 * common case that the query is simple (i.e. comprised of only one sub-query and that query is fitted, i.e. does not require
 * in memory filtering). Under these assumptions we can remove a lot of the steps in {@link org.janusgraph.graphdb.query.QueryProcessor}:
 * merging of result sets, in-memory filtering and the object instantiation required for in-memory filtering.
 * <p>
 * With those complexities removed, the query processor can be much simpler which makes it a lot faster and less
 * memory intense.
 * <p>
 * IMPORTANT: This Iterable is not thread-safe.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class SimpleVertexQueryProcessor implements Iterable<Entry> {

    private final VertexCentricQuery query;
    private final StandardJanusGraphTx tx;
    private final EdgeSerializer edgeSerializer;
    private final InternalVertex vertex;
    private final QueryProfiler profiler;

    private SliceQuery sliceQuery;

    public SimpleVertexQueryProcessor(VertexCentricQuery query, StandardJanusGraphTx tx) {
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
        Iterator<Entry> iterator;
        //If there is a limit we need to wrap the basic iterator in a LimitAdjustingIterator which ensures the right number
        //of elements is returned. Otherwise we just return the basic iterator.
        if (sliceQuery.hasLimit() && sliceQuery.getLimit()!=query.getLimit()) {
            iterator = new LimitAdjustingIterator();
        } else {
            iterator = getBasicIterator();
        }
        return iterator;
    }

    /**
     * Converts the entries from this query result into actual {@link JanusGraphRelation}.
     *
     * @return
     */
    public Iterable<JanusGraphRelation> relations() {
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
        final EntryList result = vertex.loadRelations(sliceQuery, query -> QueryProfiler.profile(profiler, query, q -> tx.getGraph().edgeQuery(vertex.longId(), q, tx.getTxHandle())));
        return result.iterator();
    }


    private final class LimitAdjustingIterator extends org.janusgraph.graphdb.query.LimitAdjustingIterator<Entry> {

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

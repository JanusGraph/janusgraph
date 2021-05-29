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

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.internal.OrderList;
import org.janusgraph.graphdb.query.BackendQueryHolder;
import org.janusgraph.graphdb.query.ElementQuery;
import org.janusgraph.graphdb.query.condition.Condition;
import org.janusgraph.graphdb.relations.RelationComparator;

import java.util.Comparator;
import java.util.List;

/**
 * A vertex-centric query which implements {@link ElementQuery} so that it can be executed by
 * {@link org.janusgraph.graphdb.query.QueryProcessor}. Most of the query definition
 * is in the extended {@link BaseVertexCentricQuery} - this class only adds the base vertex to the mix.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VertexCentricQuery extends BaseVertexCentricQuery implements ElementQuery<JanusGraphRelation, SliceQuery> {

    private final InternalVertex vertex;

    public VertexCentricQuery(InternalVertex vertex, Condition<JanusGraphRelation> condition,
                              Direction direction,
                              List<BackendQueryHolder<SliceQuery>> queries,
                              OrderList orders,
                              int limit) {
        super(condition, direction, queries, orders, limit);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    public VertexCentricQuery(InternalVertex vertex, BaseVertexCentricQuery base) {
        super(base);
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    /**
     * Constructs an empty query
     * @param vertex
     */
    protected VertexCentricQuery(InternalVertex vertex) {
        super();
        Preconditions.checkNotNull(vertex);
        this.vertex = vertex;
    }

    public static VertexCentricQuery emptyQuery(InternalVertex vertex) {
        return new VertexCentricQuery(vertex);
    }

    public InternalVertex getVertex() {
        return vertex;
    }

    @Override
    public boolean isSorted() {
        return true;
    }

    @Override
    public Comparator getSortOrder() {
        return new RelationComparator(vertex,getOrders());
    }

    @Override
    public boolean hasDuplicateResults() {
        return false; //We wanna count self-loops twice
    }

    @Override
    public String toString() {
        return vertex+super.toString();
    }

}

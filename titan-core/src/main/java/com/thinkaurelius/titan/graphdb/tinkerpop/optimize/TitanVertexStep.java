package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanVertexStep<E extends Element> extends VertexStep<E> implements HasStepFolder<Vertex,E> {

    public TitanVertexStep(VertexStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnClass(), originalStep.getDirection(), originalStep.getEdgeLabels());
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    private boolean initialized = false;
    private boolean useMultiQuery = false;

    void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
    }

    public<Q extends BaseVertexQuery> Q makeQuery(Q query) {
        query.labels(getEdgeLabels());
        query.direction(getDirection());
        for (HasContainer condition : hasContainers) {
            if (condition.predicate instanceof Contains && condition.value==null) {
                if (condition.predicate==Contains.within) query.has(condition.key);
                else query.hasNot(condition.key);
            } else {
                query.has(condition.key, TitanPredicate.Converter.convert(condition.predicate), condition.value);
            }
        }
        for (OrderEntry order : orders) query.orderBy(order.key,order.order);
        if (limit !=BaseQuery.NO_LIMIT) query.limit(limit);
        return query;
    }

    private void initialize() {
        assert !initialized;
        initialized = true;
        if (useMultiQuery) {
            if (!starts.hasNext()) throw FastNoSuchElementException.instance();
            TitanMultiVertexQuery mquery = TitanTraversalUtil.getTx(traversal).multiQuery();
            List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
            starts.forEachRemaining(v -> { vertices.add(v); mquery.addVertex(v.get()); });
            starts.add(vertices.iterator());
            assert vertices.size()>0;
            makeQuery(mquery);

            final Map<TitanVertex, Iterable<? extends TitanElement>> results =
                    (Vertex.class.isAssignableFrom(getReturnClass())) ? mquery.vertices() : mquery.edges();
            super.setFunction(v -> (Iterator<E>)results.get(v.get()).iterator());
        } else {
            super.setFunction( v -> {
                TitanVertexQuery query = makeQuery(((TitanVertex) v.get()).query());
                return (Vertex.class.isAssignableFrom(getReturnClass())) ? query.vertices().iterator() : query.edges().iterator();
            } );
        }
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    /*
    ===== HOLDER =====
     */

    private final List<HasContainer> hasContainers;
    private int limit = BaseQuery.NO_LIMIT;
    private List<OrderEntry> orders = new ArrayList<>();


    @Override
    public void addAll(Iterable<HasContainer> has) {
        Iterables.addAll(hasContainers, has);
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new OrderEntry(key,order));
    }

    @Override
    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getLimit() {
        return this.limit;
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : TraversalHelper.makeStepString(this, this.hasContainers);
    }

}

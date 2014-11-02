package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.util.FastNoSuchElementException;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanPropertiesStep<E> extends PropertiesStep<E> implements HasStepFolder<Element,E> {

    public TitanPropertiesStep(PropertiesStep<E> copy) {
        super(copy.getTraversal(), copy.getReturnType(), copy.getPropertyKeys());
        this.multiQuery = ((StandardTitanTx)traversal.sideEffects().getGraph()).getGraph().getConfiguration().useMultiQuery();
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    private final boolean multiQuery;
    private boolean initialized = false;
    private boolean isVertexProperties = false;

    private<Q extends BaseVertexQuery> Q makeQuery(Q query) {
        String[] keys = getPropertyKeys();
        if (getReturnType().forHiddens()) {
            keys = QueryUtil.hideKeys(keys);
        }
        query.keys(keys);
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

    private Iterator<E> convertIterator(Iterable<? extends TitanProperty> iterable) {
        if (getReturnType().forProperties()) return (Iterator<E>)iterable.iterator();
        assert getReturnType().forValues();
        return (Iterator<E>) Iterators.transform(iterable.iterator(), p -> p.value());
    }

    void makeVertrexProperties() {
        this.isVertexProperties = true;
    }

    private void initialize() {
        assert !initialized;
        initialized = true;
        if (!isVertexProperties) { //For edges, we leave the pipeline as is and execute the behavior of parent PropertiesStep
            assert hasContainers.isEmpty() && limit== Query.NO_LIMIT && orders.isEmpty();
            return;
        }

        if (multiQuery) {
            if (!starts.hasNext()) throw FastNoSuchElementException.instance();
            List<Traverser.Admin<Element>> elements = new ArrayList<>();
            starts.forEachRemaining(v -> elements.add(v));
            starts.add(elements.iterator());
            assert elements.size()>0;

            TitanMultiVertexQuery mquery = ((TitanTransaction) traversal.sideEffects().getGraph()).multiQuery();
            if (elements.stream().anyMatch(e -> !(e instanceof Vertex))) throw new IllegalStateException("Step should only be used against vertices");
            elements.forEach( e -> mquery.addVertex((Vertex)e.get()));
            makeQuery(mquery);

            final Map<TitanVertex, Iterable<? extends TitanProperty>> results = mquery.properties();
            super.setFunction(v -> convertIterator(results.get(v.get())));
        } else {
            super.setFunction(v -> {
                if (!(v.get() instanceof Vertex)) throw new IllegalStateException("Step should only be used against vertices");
                TitanVertexQuery query = makeQuery(((TitanVertex) v.get()).query());
                return convertIterator(query.properties());
            });
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
    private List<HasStepFolder.OrderEntry> orders = new ArrayList<>();


    @Override
    public void addAll(Iterable<HasContainer> has) {
        Iterables.addAll(hasContainers, has);
    }

    @Override
    public void orderBy(String key, Order order) {
        orders.add(new HasStepFolder.OrderEntry(key,order));
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

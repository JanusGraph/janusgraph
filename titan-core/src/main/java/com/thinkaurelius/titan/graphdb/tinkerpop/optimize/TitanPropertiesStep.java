package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanPropertiesStep<E> extends PropertiesStep<E> implements HasStepFolder<Element,E> {

    public TitanPropertiesStep(PropertiesStep<E> originalStep) {
        super(originalStep.getTraversal(), originalStep.getReturnType(), originalStep.getPropertyKeys());
        originalStep.getLabels().forEach(this::addLabel);
        this.hasContainers = new ArrayList<>();
        this.limit = Query.NO_LIMIT;
    }

    private boolean useMultiQuery = false;
    private boolean initialized = false;
    private boolean isVertexProperties = false;
    private Map<TitanVertex, Iterable<? extends TitanProperty>> multiQueryResults = null;

    void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
    }

    private<Q extends BaseVertexQuery> Q makeQuery(Q query) {
        String[] keys = getPropertyKeys();
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
        return (Iterator<E>) Iterators.transform(iterable.iterator(), p -> ((TitanProperty)p).value());
    }

    void makeVertrexProperties() {
        this.isVertexProperties = true;
    }

    @SuppressWarnings("deprecation")
    private void initialize() {
        assert !initialized;
        initialized = true;
        if (!isVertexProperties) { //For edges, we leave the pipeline as is and execute the behavior of parent PropertiesStep
            assert hasContainers.isEmpty() && limit== Query.NO_LIMIT && orders.isEmpty();
            return;
        }

        if (useMultiQuery) {
            if (!starts.hasNext()) throw FastNoSuchElementException.instance();
            List<Traverser.Admin<Element>> elements = new ArrayList<>();
            starts.forEachRemaining(v -> elements.add(v));
            starts.add(elements.iterator());
            assert elements.size()>0;

            TitanMultiVertexQuery mquery = TitanTraversalUtil.getTx(traversal).multiQuery();
            if (elements.stream().anyMatch(e -> !(e instanceof Vertex))) throw new IllegalStateException("Step should only be used against vertices");
            elements.forEach( e -> mquery.addVertex((Vertex)e.get()));
            makeQuery(mquery);

            multiQueryResults = mquery.properties();
        }
    }

    @Override
    protected Traverser<E> processNextStart() {
        //if (!initialized) initialize();
        return super.processNextStart();
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<Element> traverser) {
//        if (useMultiQuery) {
//            assert multiQueryResults!=null;
//            return (Iterator<E>)multiQueryResults.get(traverser.get()).iterator();
//        } else {
            return super.flatMap(traverser);
//        }
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    @Override
    public TitanPropertiesStep<E> clone() {
        final TitanPropertiesStep<E> clone = (TitanPropertiesStep<E>) super.clone();
        clone.initialized=false;
        return clone;
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

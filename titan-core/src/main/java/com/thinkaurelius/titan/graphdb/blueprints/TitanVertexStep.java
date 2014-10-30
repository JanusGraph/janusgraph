package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.Traverser;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.*;
import java.util.function.Function;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanVertexStep<E extends Element> extends VertexStep<E> implements HasStepFolder<Vertex,E> {

    private final boolean multiQuery;
    private final List<HasContainer> hasContainers;
    private int branchFactor = BaseQuery.NO_LIMIT;
    private List<OrderEntry> orders = new ArrayList<>();


    public TitanVertexStep(Traversal traversal, Class<E> returnClass, Direction direction, int branchFactor, String... edgeLabels) {
        super(traversal, returnClass, direction, branchFactor, edgeLabels);
        this.multiQuery = false; //((StandardTitanTx)traversal.sideEffects().getGraph()).getGraph().getConfiguration().useMultiQuery();
        this.hasContainers = new ArrayList<>();
    }

    private TitanVertexStep(TitanVertexStep copy, Class<E> returnClass) {
        super(copy.getTraversal(), returnClass, copy.getDirection(), copy.getBranchFactor(), copy.getEdgeLabels());
        this.multiQuery = copy.multiQuery;
        this.hasContainers = copy.hasContainers;
    }

    TitanVertexStep<Vertex> makeVertexStep() {
        assert isEdgeStep();
        return new TitanVertexStep<Vertex>(this,Vertex.class);
    }

    public boolean isEdgeStep() {
        return Edge.class.isAssignableFrom(getReturnClass());
    }

    private Iterator<Traverser<E>> iterator = Collections.emptyIterator();
    private Iterator<Traverser.Admin<Vertex>> incoming;
    private Function<Vertex,Iterator<E>> adjFunction;
    private boolean initialized = false;

    private<Q extends BaseVertexQuery> Q makeQuery(Q query) {
        for (HasContainer condition : hasContainers) {
            query.has(condition.key,TitanPredicate.Converter.convert(condition.predicate),condition.value);
        }
        for (OrderEntry order : orders) query.orderBy(order.key,order.order);
        if (branchFactor!=BaseQuery.NO_LIMIT) query.limit(branchFactor);
        return query;
    }

    private void initialize() {
        assert !initialized;
        initialized = true;
        if (multiQuery) {
            if (!starts.hasNext()) starts.next();
            TitanMultiVertexQuery mquery = ((TitanTransaction)traversal.sideEffects().getGraph()).multiQuery();
            List<Traverser.Admin<Vertex>> vertices = new ArrayList<>();
            starts.forEachRemaining(v -> { vertices.add(v); mquery.addVertex(v.get()); });
            assert vertices.size()>0;
            makeQuery(mquery);

            final Map<TitanVertex, Iterable<? extends TitanElement>> results =
                    (Vertex.class.isAssignableFrom(getReturnClass())) ? mquery.vertices() : mquery.edges();

            incoming = vertices.iterator();
            adjFunction = ( v -> (Iterator<E>)results.get(v).iterator());
        } else {
            incoming = starts;
            adjFunction = ( v -> {
                TitanVertexQuery query = makeQuery(((TitanVertex) v).query());
                return (Vertex.class.isAssignableFrom(getReturnClass())) ? query.vertices().iterator() : query.edges().iterator();
            } );
        }
    }

    @Override
    protected Traverser<E> processNextStart() {
        if (!initialized) initialize();
        while (true) {
            if (this.iterator.hasNext())
                return this.iterator.next(); // timer start/finish in next() call
            else {
                final Traverser.Admin<Vertex> traverser = incoming.next();
                this.iterator = new IndividualVertexIterator<>(traverser, this, adjFunction.apply(traverser.get()));
            }
        }
    }

    private static final class IndividualVertexIterator<B> implements Iterator<Traverser<B>> {

        private final Traverser.Admin<Vertex> head;
        private final Iterator<B> iterator;
        private final Step step;

        private IndividualVertexIterator(final Traverser.Admin<Vertex> head, final Step step, final Iterator<B> iterator) {
            this.iterator = iterator;
            this.head = head;
            this.step = step;
        }

        @Override
        public final boolean hasNext() {
            return this.iterator.hasNext();
        }

        @Override
        public final Traverser<B> next() {
            final Traverser.Admin<B> traverser = this.head.makeChild(this.step.getLabel(), this.iterator.next());
            return traverser;
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
        this.iterator = Collections.emptyIterator();
    }


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
        this.branchFactor = limit;
    }

    @Override
    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : TraversalHelper.makeStepString(this, this.hasContainers);
    }

    @Override
    public int getBranchFactor() {
        return this.branchFactor;
    }

}

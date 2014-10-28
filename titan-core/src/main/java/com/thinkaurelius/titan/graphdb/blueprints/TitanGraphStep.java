package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanGraphQuery;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.graphdb.query.TitanPredicate;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.util.TraversalHelper;
import com.tinkerpop.gremlin.structure.Compare;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import com.tinkerpop.gremlin.structure.util.HasContainer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanGraphStep<E extends Element> extends GraphStep<E> {

    public final List<HasContainer> hasContainers = new ArrayList<>();

    public TitanGraphStep(final Traversal traversal, final Class<E> returnClass) {
        super(traversal, returnClass);
    }

    @Override
    public void generateTraverserIterator(final boolean trackPaths) {
        //TODO: construct GraphQuery
        TitanTransaction tx = (TitanTransaction)this.traversal.sideEffects().getGraph();
        TitanGraphQuery query = tx.query();
        for (HasContainer condition : hasContainers) {
            query.has(condition.key,TitanPredicate.Converter.convert(condition.predicate),condition.value);
        }
        this.start = Vertex.class.isAssignableFrom(this.returnClass) ? query.vertices().iterator() : query.edges().iterator();
        super.generateTraverserIterator(trackPaths);
    }

    public String toString() {
        return this.hasContainers.isEmpty() ? super.toString() : TraversalHelper.makeStepString(this, this.hasContainers);
    }

}


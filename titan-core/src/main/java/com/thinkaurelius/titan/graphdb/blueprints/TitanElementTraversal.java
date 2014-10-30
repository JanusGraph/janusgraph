package com.thinkaurelius.titan.graphdb.blueprints;

import com.thinkaurelius.titan.core.TitanGraphTransaction;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.graph.GraphTraversal;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;
import com.tinkerpop.gremlin.structure.Element;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanElementTraversal<S, E>  extends DefaultGraphTraversal<S, E> {

    public TitanElementTraversal(final Element element, final TitanTransaction graph) {
        super(graph);
        getStrategies().register(TitanVertexStepStrategy.instance());
        addStep(new StartStep<>(this, element));
    }

    @Override
    public <E2> GraphTraversal<S, E2> addStep(Step<?, E2> step) {
        if (this.getStrategies().complete()) throw Exceptions.traversalIsLocked();
        return super.addStep(TitanTraversal.replaceStep(this,step));
    }

}
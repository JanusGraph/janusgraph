package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.thinkaurelius.titan.core.TitanTransaction;
import com.tinkerpop.gremlin.process.Step;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.map.PropertiesStep;
import com.tinkerpop.gremlin.process.graph.step.map.VertexStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.StartStep;
import com.tinkerpop.gremlin.process.graph.util.DefaultGraphTraversal;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanTraversal<S, E> extends DefaultGraphTraversal<S, E> {

    public TitanTraversal(final TitanTransaction graph) {
        super(graph);
        addStep(new StartStep<>(this));
    }

    public static Step replaceStep(final Step step) {
        if (step instanceof VertexStep) {
            VertexStep vstep = (VertexStep)step;
            return new TitanVertexStep(vstep);
        } else if (step instanceof PropertiesStep) {
            PropertiesStep sstep = (PropertiesStep)step;
            return new TitanPropertiesStep<>(sstep);
        } else {
            return step;
        }
    }

}

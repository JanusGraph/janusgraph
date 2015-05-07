package com.thinkaurelius.titan.graphdb.tinkerpop.optimize;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

/**
 * Created by bryn on 06/05/15.
 */
public class TitanIdsValidationStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {


    private static final TitanIdsValidationStrategy INSTANCE = new TitanIdsValidationStrategy();



    @Override
    public void apply(Traversal.Admin traversal) {
        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(step -> {
            boolean elementFound = false;
            boolean idFound = false;
            if (step.returnsVertices()) {
                for (Object id : step.getIds()) {
                    if (id instanceof Long) {
                        idFound = true;
                    } else if (id instanceof Vertex) {
                        elementFound = true;
                    } else {

                    }
                }
            }
            if (step.returnsEdges()) {
                for (Object id : step.getIds()) {
                    if (id instanceof RelationIdentifier) {
                        idFound = true;
                    } else if (id instanceof Edge) {
                        elementFound = true;
                    } else {

                    }
                }
            }
            if (elementFound && idFound) {
                throw Graph.Exceptions.idArgsMustBeEitherIdOrElement();
            }
        });
    }

    public static TitanIdsValidationStrategy instance() {
            return INSTANCE;
    }
}

// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb.tinkerpop.optimize.strategy;

import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;

import static org.janusgraph.graphdb.types.system.ImplicitKey.ADJACENT_ID;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexHasIdOptimizerStrategy
    extends AdjacentVertexOptimizerStrategy<HasStep<?>> {

    private static final AdjacentVertexHasIdOptimizerStrategy INSTANCE =
        new AdjacentVertexHasIdOptimizerStrategy();

    private AdjacentVertexHasIdOptimizerStrategy() {}

    public static AdjacentVertexHasIdOptimizerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(HasStep.class, traversal)
            .forEach(this::optimizeStep);
    }

    private P<?> parsePredicate(HasStep<?> hasStep) {
        List<HasContainer> hasContainers = hasStep.getHasContainers();

        if (hasContainers.size() != 1) {
            return null; // TODO does it make sense to allow steps with >1 containers here?
        }

        HasContainer hc = hasContainers.get(0);
        if (hc.getKey().equals(T.id.getAccessor())) {
            return hc.getPredicate();
        } else {
            // we are not interested in predicates for other keys
            return null;
        }
    }

    @Override
    protected boolean isValidStep(HasStep<?> step) {
        P predicate = parsePredicate(step);
        if (predicate == null) {
            return false;
        }

        if (predicate.getBiPredicate() != Compare.eq) {
            return false;
        }

        Object predicateValue = predicate.getValue();
        return predicateValue instanceof Vertex || predicateValue instanceof Long;
    }

    @Override
    protected FilterStep<Edge> makeFilterByAdjacentIdStep(Traversal.Admin<?, ?> traversal, HasStep<?> originalStep) {
        HasContainer hc = new HasContainer(ADJACENT_ID.name(), P.eq(parsePredicate(originalStep).getValue()));
        return new HasStep<>(traversal, hc);
    }
}

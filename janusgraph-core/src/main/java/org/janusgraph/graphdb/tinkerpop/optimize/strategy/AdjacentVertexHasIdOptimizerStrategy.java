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

import java.util.List;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexHasIdOptimizerStrategy
    extends AdjacentVertexOptimizerStrategy<HasStep<?>> {

    private static final AdjacentVertexHasIdOptimizerStrategy INSTANCE =
        new AdjacentVertexHasIdOptimizerStrategy();

    private AdjacentVertexHasIdOptimizerStrategy() {}

    public static AdjacentVertexHasIdOptimizerStrategy instance() { return INSTANCE; }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(HasStep.class, traversal)
            .forEach(step -> optimizeStep(step));
    }

    @Override
    protected P<?> parsePredicate(HasStep<?> hasStep) {
        List<HasContainer> hasContainers = hasStep.getHasContainers();

        if (hasContainers.size() != 1) {
            return null; // TODO does it make sense to allow steps with >1 containers here?
        }

        HasContainer hc = hasStep.getHasContainers().get(0);
        if (hc.getKey().equals(T.id.getAccessor())) {
            return hc.getPredicate();
        } else {
            // we are not interested in predicates for other keys
            return null;
        }
    }

    @Override
    protected boolean isValidPredicate(P<?> predicate) {
        if (predicate == null) {
            return false;
        }

        if (predicate.getBiPredicate() != Compare.eq) {
            return false;
        }

        Object predicateValue = predicate.getValue();
        return predicateValue instanceof Vertex || predicateValue instanceof Long;
    }
}

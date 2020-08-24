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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.FilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.IsStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import static org.janusgraph.graphdb.types.system.ImplicitKey.ADJACENT_ID;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class AdjacentVertexIsOptimizerStrategy extends AdjacentVertexOptimizerStrategy<IsStep<?>> {

    private static final AdjacentVertexIsOptimizerStrategy INSTANCE =
        new AdjacentVertexIsOptimizerStrategy();

    private AdjacentVertexIsOptimizerStrategy() {}

    public static AdjacentVertexIsOptimizerStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfClass(IsStep.class, traversal)
            .forEach(this::optimizeStep);
    }

    @Override
    protected boolean isValidStep(IsStep<?> step) {
        P<?> predicate = step.getPredicate();
        return predicate.getValue() instanceof Vertex;
    }

    @Override
    protected FilterStep<Edge> makeFilterByAdjacentIdStep(Traversal.Admin<?, ?> traversal, IsStep<?> originalStep) {
        HasContainer hc = new HasContainer(ADJACENT_ID.name(), P.eq(originalStep.getPredicate().getValue()));
        return new HasStep<>(traversal, hc);
    }
}

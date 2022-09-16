// Copyright 2017 JanusGraph Authors
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

import org.apache.commons.lang.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphStep;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphStepStrategy INSTANCE = new JanusGraphStepStrategy();

    private JanusGraphStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        TraversalHelper.getStepsOfClass(GraphStep.class, traversal).forEach(originalGraphStep -> {
            if (ArrayUtils.isEmpty(originalGraphStep.getIds())) {
                //Try to optimize for index calls
                final JanusGraphStep<?, ?> janusGraphStep = new JanusGraphStep<>(originalGraphStep);
                TraversalHelper.replaceStep(originalGraphStep, janusGraphStep, traversal);
                //Fold in steps containing ids like hasId(ids) and has(T.id, P.within(ids))
                HasStepFolder.foldInIds(janusGraphStep, traversal);
                //Fold in other "has" steps like has(key) and has(key, value)
                HasStepFolder.foldInHasContainer(janusGraphStep, traversal, traversal);
                //Now that has(T.id, ids) and hasId(ids) steps are folded, we check again
                //if there are any ids folded. If yes, then we shouldn't fold in order and
                //range steps so that they can be handled by TinkerPop.
                if (ArrayUtils.isEmpty(janusGraphStep.getIds())) {
                    //Fold in ordering steps like order().by(key) step
                    HasStepFolder.foldInOrder(janusGraphStep, janusGraphStep.getNextStep(), traversal, traversal, janusGraphStep.returnsVertex(), null);
                    //Fold in range steps like limit(number) and range(lowLimit, highLimit)
                    HasStepFolder.foldInRange(janusGraphStep, JanusGraphTraversalUtil.getNextNonIdentityStep(janusGraphStep), traversal, null);
                }
            } else {
                //Make sure that any provided "start" elements are instantiated in the current transaction
                final Object[] ids = originalGraphStep.getIds();
                final Object[] elementIds = new Object[ids.length];
                for (int i = 0; i < ids.length; i++) {
                    if (ids[i] instanceof Element) {
                        elementIds[i] = ((Element) ids[i]).id();
                    }
                    else
                    {
                        elementIds[i] = ids[i];
                    }
                }
                originalGraphStep.setIteratorSupplier(() -> originalGraphStep.returnsVertex() ?
                    ((Graph) originalGraphStep.getTraversal().getGraph().get()).vertices(elementIds) :
                    ((Graph) originalGraphStep.getTraversal().getGraph().get()).edges(elementIds));
            }

        });
    }

    public static JanusGraphStepStrategy instance() {
        return INSTANCE;
    }
}

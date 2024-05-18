// Copyright 2023 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Removes any `JanusGraphMultiQueryStep` without any registered `clientSteps`. When removing such
 * `JanusGraphMultiQueryStep` this strategy also removes any barrier step created in `JanusGraphMultiQueryStrategy`
 * for the respected `JanusGraphMultiQueryStep`. This strategy doesn't remove any barrier step which was explicitly created
 * (i.e. those, which are not in direct relation with the unused `JanusGraphMultiQueryStep`).
 */
public class JanusGraphUnusedMultiQueryRemovalStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(JanusGraphMultiQueryStrategy.class));
    private static final JanusGraphUnusedMultiQueryRemovalStrategy INSTANCE = new JanusGraphUnusedMultiQueryRemovalStrategy();

    private JanusGraphUnusedMultiQueryRemovalStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.getGraph().isPresent())
            return;

        final StandardJanusGraph janusGraph = JanusGraphTraversalUtil.getJanusGraph(traversal);
        if (janusGraph == null) {
            return;
        }

        final Optional<StandardJanusGraphTx> tx = JanusGraphTraversalUtil.getJanusGraphTx(traversal);
        boolean useMultiQuery = tx.isPresent() ? tx.get().getConfiguration().useMultiQuery() : janusGraph.getConfiguration().useMultiQuery();
        if (!useMultiQuery) {
            return;
        }

        List<JanusGraphMultiQueryStep> unusedMultiQuerySteps = findUnusedMultiQuerySteps(traversal);
        unusedMultiQuerySteps.forEach(multiQueryStep -> {
            Step generatedBarrierStep = multiQueryStep.getGeneratedBarrierStep();
            traversal.removeStep(multiQueryStep);
            if(generatedBarrierStep != null){
                traversal.removeStep(generatedBarrierStep);
            }
        });
    }

    private List<JanusGraphMultiQueryStep> findUnusedMultiQuerySteps(final Traversal.Admin<?, ?> traversal){
        List<JanusGraphMultiQueryStep> unusedMultiQuerySteps = new ArrayList<>();
        traversal.getSteps().forEach(step -> {
            if(step instanceof JanusGraphMultiQueryStep
                && ((JanusGraphMultiQueryStep) step).isSameLoopClientStepsEmpty()
                && ((JanusGraphMultiQueryStep) step).isNextLoopClientStepsEmpty()
                && ((JanusGraphMultiQueryStep) step).isFirstLoopClientStepsEmpty()){
                unusedMultiQuerySteps.add((JanusGraphMultiQueryStep) step);
            }
        });
        return unusedMultiQuerySteps;
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static JanusGraphUnusedMultiQueryRemovalStrategy instance() {
        return INSTANCE;
    }
}

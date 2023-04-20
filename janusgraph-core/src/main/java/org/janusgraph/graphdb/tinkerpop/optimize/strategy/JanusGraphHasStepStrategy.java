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
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphHasStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class JanusGraphHasStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphHasStepStrategy INSTANCE = new JanusGraphHasStepStrategy();

    private JanusGraphHasStepStrategy() {
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
        final MultiQueryHasStepStrategyMode hasStepStrategyMode;
        final int txVertexCacheSize;
        final boolean hasPropertyPrefetching;
        if(tx.isPresent()){
            TransactionConfiguration txConfig = tx.get().getConfiguration();
            hasStepStrategyMode = txConfig.getHasStepStrategyMode();
            txVertexCacheSize = txConfig.getVertexCacheSize();
            hasPropertyPrefetching = txConfig.hasPropertyPrefetching();
        } else {
            GraphDatabaseConfiguration graphConfig = janusGraph.getConfiguration();
            hasStepStrategyMode = graphConfig.hasStepStrategyMode();
            txVertexCacheSize = graphConfig.getTxVertexCacheSize();
            hasPropertyPrefetching = graphConfig.hasPropertyPrefetching();
        }

        if(MultiQueryHasStepStrategyMode.NONE.equals(hasStepStrategyMode)){
            return;
        }

        // if `hasPropertyPrefetching` is `true` than modes which don't fetch all properties are redundant because
        // the cached slice queries for separate properties are not going to be used for a query which accesses all properties together.
        // Thus, having anything than `ALL_PROPERTIES` - mean additional redundant requests.
        // That's why we use `MultiQueryHasStepStrategyMode.ALL_PROPERTIES` whenever `hasPropertyPrefetching` is `true`.
        applyJanusGraphHasSteps(traversal, txVertexCacheSize,
            hasPropertyPrefetching ? MultiQueryHasStepStrategyMode.ALL_PROPERTIES : hasStepStrategyMode);
    }

    private void applyJanusGraphHasSteps(final Traversal.Admin<?, ?> traversal, final int txVertexCacheSize,
                                         final MultiQueryHasStepStrategyMode hasStepStrategyMode) {
        TraversalHelper.getStepsOfAssignableClass(HasStep.class, traversal).forEach(originalStep -> {
            if(originalStep instanceof JanusGraphHasStep){
                return;
            }
            final JanusGraphHasStep janusGraphHasStep = createJanusGraphHasStep(originalStep, txVertexCacheSize, hasStepStrategyMode);
            TraversalHelper.replaceStep(originalStep, janusGraphHasStep, originalStep.getTraversal());
        });
    }

    private JanusGraphHasStep createJanusGraphHasStep(final HasStep originalStep,
                                                      final int txVertexCacheSize,
                                                      final MultiQueryHasStepStrategyMode hasStepStrategyMode){
        final JanusGraphHasStep janusGraphHasStep = new JanusGraphHasStep(originalStep);
        janusGraphHasStep.setTxVertexCacheSize(txVertexCacheSize);
        if(MultiQueryHasStepStrategyMode.ALL_PROPERTIES.equals(hasStepStrategyMode)){
            janusGraphHasStep.setPrefetchAllPropertiesRequired(true);
        } else if(MultiQueryHasStepStrategyMode.REQUIRED_AND_NEXT_PROPERTIES.equals(hasStepStrategyMode) ||
            MultiQueryHasStepStrategyMode.REQUIRED_AND_NEXT_PROPERTIES_OR_ALL.equals(hasStepStrategyMode)){
            Optional<Set<String>> optionalNextStepNeededProperties = findNextStepNeededProperties(originalStep);
            if(optionalNextStepNeededProperties.isPresent()){
                Set<String> nextStepNeededProperties = optionalNextStepNeededProperties.get();
                if(nextStepNeededProperties.isEmpty()){
                    janusGraphHasStep.setPrefetchAllPropertiesRequired(true);
                } else {
                    for(String nextStepNeededProperty : nextStepNeededProperties){
                        janusGraphHasStep.withPropertyPrefetch(nextStepNeededProperty);
                    }
                }
            } else if(MultiQueryHasStepStrategyMode.REQUIRED_AND_NEXT_PROPERTIES_OR_ALL.equals(hasStepStrategyMode)){
                janusGraphHasStep.setPrefetchAllPropertiesRequired(true);
            }
        }
        return janusGraphHasStep;
    }

    private Optional<Set<String>> findNextStepNeededProperties(final HasStep originalStep){
        Step nextStep = originalStep.getNextStep();
        while (nextStep instanceof NoOpBarrierStep || nextStep instanceof IdentityStep
            || nextStep instanceof ProfileStep || nextStep instanceof SideEffectStep
            || nextStep instanceof JanusGraphMultiQueryStep){
            nextStep = nextStep.getNextStep();
        }
        if(nextStep instanceof PropertiesStep){
            return Optional.of(new HashSet<>(Arrays.asList(((PropertiesStep) nextStep).getPropertyKeys())));
        }
        if(nextStep instanceof PropertyMapStep){
            return Optional.of(new HashSet<>(Arrays.asList(((PropertyMapStep) nextStep).getPropertyKeys())));
        }
        if(nextStep instanceof ElementMapStep){
            return Optional.of(new HashSet<>(Arrays.asList(((ElementMapStep) nextStep).getPropertyKeys())));
        }
        return Optional.empty();
    }

    public static JanusGraphHasStepStrategy instance() {
        return INSTANCE;
    }

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = Collections.singleton(JanusGraphLocalQueryOptimizerStrategy.class);

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }
}

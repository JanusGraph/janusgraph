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
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.HasStepFolder;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphElementMapStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphLabelStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertiesStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphPropertyMapStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphVertexStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.transaction.TransactionConfiguration;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class JanusGraphLocalQueryOptimizerStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final JanusGraphLocalQueryOptimizerStrategy INSTANCE = new JanusGraphLocalQueryOptimizerStrategy();

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = Collections.singleton(AdjacentVertexFilterOptimizerStrategy.class);

    private JanusGraphLocalQueryOptimizerStrategy() {
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
        final MultiQueryPropertiesStrategyMode propertiesStrategyMode;
        final MultiQueryLabelStepStrategyMode labelStepStrategyMode;
        final int txVertexCacheSize;

        if(tx.isPresent()){
            TransactionConfiguration txConfig = tx.get().getConfiguration();
            txVertexCacheSize = txConfig.getVertexCacheSize();
            propertiesStrategyMode = txConfig.getPropertiesStrategyMode();
            labelStepStrategyMode = txConfig.getLabelStepStrategyMode();
        } else {
            GraphDatabaseConfiguration graphConfig = janusGraph.getConfiguration();
            txVertexCacheSize = graphConfig.getTxVertexCacheSize();
            propertiesStrategyMode = graphConfig.propertiesStrategyMode();
            labelStepStrategyMode = graphConfig.labelStepStrategyMode();
        }

        applyJanusGraphVertexSteps(traversal);
        applyJanusGraphPropertiesSteps(traversal, txVertexCacheSize, propertiesStrategyMode);
        applyJanusGraphLabelSteps(traversal, labelStepStrategyMode);
        inspectLocalTraversals(traversal, txVertexCacheSize, propertiesStrategyMode);
    }

    private void applyJanusGraphVertexSteps(Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal).forEach(originalStep -> {
            final JanusGraphVertexStep vertexStep = new JanusGraphVertexStep(originalStep);
            TraversalHelper.replaceStep(originalStep, vertexStep, originalStep.getTraversal());

            if (JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep)) {
                HasStepFolder.foldInHasContainer(vertexStep, originalStep.getTraversal(), originalStep.getTraversal());
                //We cannot fold in orders or ranges since they are not local
            }

            assert JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep) || JanusGraphTraversalUtil.isVertexReturnStep(vertexStep);
            final Step nextStep = JanusGraphTraversalUtil.getNextNonIdentityStep(vertexStep);
            if (nextStep instanceof RangeGlobalStep) {
                final int limit = QueryUtil.convertLimit(((RangeGlobalStep) nextStep).getHighRange());
                vertexStep.setLimit(0, QueryUtil.mergeHighLimits(limit, vertexStep.getHighLimit()));
            }
        });
    }

    private void applyJanusGraphPropertiesSteps(Traversal.Admin<?, ?> traversal, int txVertexCacheSize, MultiQueryPropertiesStrategyMode propertiesStrategyMode) {
        JanusGraphTraversalUtil.getSteps(step -> step instanceof PropertiesStep || step instanceof PropertyMapStep || step instanceof ElementMapStep, traversal).forEach(originalStep -> {
            if(originalStep instanceof MultiQueriable){
                return;
            }

            final MultiQueriable propertiesPrefetchingStep;

            boolean prefetchAllPropertiesRequired = MultiQueryPropertiesStrategyMode.ALL_PROPERTIES.equals(propertiesStrategyMode);
            boolean prefetchAllowed = !MultiQueryPropertiesStrategyMode.NONE.equals(propertiesStrategyMode);

            if(originalStep instanceof PropertiesStep){

                final JanusGraphPropertiesStep propertiesStep = new JanusGraphPropertiesStep((PropertiesStep) originalStep,
                    prefetchAllPropertiesRequired, prefetchAllowed);
                TraversalHelper.replaceStep(originalStep, propertiesStep, originalStep.getTraversal());
                if (propertiesStep.getReturnType().forProperties()) {
                    HasStepFolder.foldInHasContainer(propertiesStep, originalStep.getTraversal(), originalStep.getTraversal());
                    //We cannot fold in orders or ranges since they are not local
                }
                propertiesPrefetchingStep = propertiesStep;

            } else if(originalStep instanceof PropertyMapStep){

                final JanusGraphPropertyMapStep propertyMapStep = new JanusGraphPropertyMapStep((PropertyMapStep) originalStep,
                    prefetchAllPropertiesRequired, prefetchAllowed);
                TraversalHelper.replaceStep(originalStep, propertyMapStep, originalStep.getTraversal());
                propertiesPrefetchingStep = propertyMapStep;

            } else if(originalStep instanceof ElementMapStep){

                final JanusGraphElementMapStep elementMapStep = new JanusGraphElementMapStep((ElementMapStep) originalStep,
                    prefetchAllPropertiesRequired, prefetchAllowed);
                TraversalHelper.replaceStep(originalStep, elementMapStep, originalStep.getTraversal());
                propertiesPrefetchingStep = elementMapStep;

            } else {
                return;
            }

            propertiesPrefetchingStep.setBatchSize(txVertexCacheSize);

        });
    }

    private void inspectLocalTraversals(final Traversal.Admin<?, ?> traversal, int txVertexCacheSize, MultiQueryPropertiesStrategyMode propertiesStrategyMode) {
        TraversalHelper.getStepsOfClass(LocalStep.class, traversal).forEach(localStep -> {
            final Traversal.Admin localTraversal = ((LocalStep<?, ?>) localStep).getLocalChildren().get(0);
            final Step localStart = localTraversal.getStartStep();

            if (localStart instanceof VertexStep) {
                final JanusGraphVertexStep vertexStep = new JanusGraphVertexStep((VertexStep) localStart);
                vertexStep.setBatchSize(txVertexCacheSize);
                TraversalHelper.replaceStep(localStart, vertexStep, localTraversal);

                if (JanusGraphTraversalUtil.isEdgeReturnStep(vertexStep)) {
                    HasStepFolder.foldInHasContainer(vertexStep, localTraversal, traversal);
                    HasStepFolder.foldInOrder(vertexStep, vertexStep.getNextStep(), localTraversal, traversal, false, null);
                }
                HasStepFolder.foldInRange(vertexStep, JanusGraphTraversalUtil.getNextNonIdentityStep(vertexStep), localTraversal, null);
                unfoldLocalTraversal(traversal, localStep, localTraversal, vertexStep);

            } else if (localStart instanceof PropertiesStep) {

                boolean prefetchAllPropertiesRequired = MultiQueryPropertiesStrategyMode.ALL_PROPERTIES.equals(propertiesStrategyMode);
                boolean prefetchAllowed = !MultiQueryPropertiesStrategyMode.NONE.equals(propertiesStrategyMode);
                final JanusGraphPropertiesStep propertiesStep = new JanusGraphPropertiesStep((PropertiesStep) localStart, prefetchAllPropertiesRequired, prefetchAllowed);
                propertiesStep.setBatchSize(txVertexCacheSize);

                TraversalHelper.replaceStep(localStart, propertiesStep, localTraversal);

                if (propertiesStep.getReturnType().forProperties()) {
                    HasStepFolder.foldInHasContainer(propertiesStep, localTraversal, traversal);
                    HasStepFolder.foldInOrder(propertiesStep, propertiesStep.getNextStep(), localTraversal, traversal, false, null);
                }
                HasStepFolder.foldInRange(propertiesStep, JanusGraphTraversalUtil.getNextNonIdentityStep(propertiesStep), localTraversal, null);
                unfoldLocalTraversal(traversal, localStep, localTraversal, propertiesStep);
            }
        });
    }

    private void applyJanusGraphLabelSteps(Traversal.Admin<?, ?> traversal, MultiQueryLabelStepStrategyMode labelStepStrategyMode){
        if(MultiQueryLabelStepStrategyMode.NONE.equals(labelStepStrategyMode)){
            return;
        }
        TraversalHelper.getStepsOfAssignableClass(LabelStep.class, traversal).forEach(originalStep -> {
            if(originalStep instanceof JanusGraphLabelStep){
                return;
            }
            final JanusGraphLabelStep janusGraphLabelStep = new JanusGraphLabelStep(originalStep);
            TraversalHelper.replaceStep(originalStep, janusGraphLabelStep, originalStep.getTraversal());
        });
    }

    private static void unfoldLocalTraversal(final Traversal.Admin<?, ?> traversal,
                                             LocalStep<?, ?> localStep, Traversal.Admin localTraversal,
                                             MultiQueriable vertexStep) {
        assert localTraversal.asAdmin().getSteps().size() > 0;
        if (localTraversal.asAdmin().getSteps().size() == 1) {
            //Can replace the entire localStep by the vertex step in the outer traversal
            assert localTraversal.getStartStep() == vertexStep;
            vertexStep.setTraversal(traversal);
            TraversalHelper.replaceStep(localStep, vertexStep, traversal);
        }
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static JanusGraphLocalQueryOptimizerStrategy instance() {
        return INSTANCE;
    }
}

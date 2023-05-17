// Copyright 2021 JanusGraph Authors
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
import org.apache.tinkerpop.gremlin.process.traversal.Traversal.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.tinkerpop.optimize.JanusGraphTraversalUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.MultiQueryPositions;
import org.janusgraph.graphdb.tinkerpop.optimize.step.JanusGraphMultiQueryStep;
import org.janusgraph.graphdb.tinkerpop.optimize.step.MultiQueriable;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 * @author Matthias Broecheler (http://matthiasb.com)
 */
public class JanusGraphMultiQueryStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final Set<Class<? extends ProviderOptimizationStrategy>> PRIORS = new HashSet<>(Arrays.asList(JanusGraphLocalQueryOptimizerStrategy.class, JanusGraphStepStrategy.class));
    private static final JanusGraphMultiQueryStrategy INSTANCE = new JanusGraphMultiQueryStrategy();

    private JanusGraphMultiQueryStrategy() {
    }

    @Override
    public void apply(final Admin<?, ?> traversal) {
        if (!traversal.getGraph().isPresent()
            || TraversalHelper.onGraphComputer(traversal)
            // The LazyBarrierStrategy is not allowed to run on traversals which use drop(). As a precaution,
            // this strategy should not run on those traversals either, because it can also insert barrier().
            || !TraversalHelper.getStepsOfAssignableClassRecursively(DropStep.class, traversal).isEmpty()) {
            return;
        }

        final StandardJanusGraph janusGraph = JanusGraphTraversalUtil.getJanusGraph(traversal);
        if (janusGraph == null) {
            return;
        }

        final Optional<StandardJanusGraphTx> tx = JanusGraphTraversalUtil.getJanusGraphTx(traversal);
        boolean useMultiQuery = tx.isPresent() ? tx.get().getConfiguration().useMultiQuery() : janusGraph.getConfiguration().useMultiQuery();
        if (!useMultiQuery) {
            return;
        }

        GraphDatabaseConfiguration graphConfig = janusGraph.getConfiguration();
        boolean limitedBatch = graphConfig.limitedBatch();
        int limitedBatchSize = graphConfig.limitedBatchSize();

        insertMultiQuerySteps(traversal, limitedBatch, limitedBatchSize);
        configureMultiQueriables(traversal, limitedBatch, limitedBatchSize);
    }

    /**
     * Insert JanusGraphMultiQuerySteps everywhere in the current traversal where MultiQueriable steps could benefit
     *
     * @param traversal The local traversal layer.
     */
    private void insertMultiQuerySteps(final Admin<?, ?> traversal, boolean limitedBatch, int limitedBatchSize) {
        JanusGraphTraversalUtil.getSteps(JanusGraphTraversalUtil::isMultiQueryCompatibleStep, traversal).forEach(step -> {
            if(!JanusGraphTraversalUtil.isRepeatChildTraversalStartStep(step)){
                Optional<Step> multiQueryPosition = JanusGraphTraversalUtil.getLocalMultiQueryPositionForStep(step);
                if (multiQueryPosition.isPresent() && JanusGraphTraversalUtil.isLegalMultiQueryPosition(multiQueryPosition.get())) {
                    insertMultiQueryStep(multiQueryPosition.get(), limitedBatch, limitedBatchSize);
                }
            }
            if(step instanceof RepeatStep){
                insertMultiQueryStepToTraversalEnd(((RepeatStep) step).getRepeatTraversal(), limitedBatch, limitedBatchSize);
            }
        });
    }

    private void insertMultiQueryStep(Step position, boolean limitedBatch, int limitedBatchSize){
        final Admin<?, ?> traversal = position.getTraversal();
        final JanusGraphMultiQueryStep multiQueryStep;
        if(limitedBatch){
            if(position instanceof NoOpBarrierStep){
                multiQueryStep = new JanusGraphMultiQueryStep(traversal, limitedBatch,
                    ((NoOpBarrierStep) position).getMaxBarrierSize());
            } else {
                NoOpBarrierStep barrier = new NoOpBarrierStep(traversal, limitedBatchSize);
                TraversalHelper.insertBeforeStep(barrier, position, traversal);
                position = barrier;
                multiQueryStep = new JanusGraphMultiQueryStep(traversal, limitedBatch, barrier);
            }
        } else {
            multiQueryStep = new JanusGraphMultiQueryStep(traversal, limitedBatch);
        }
        TraversalHelper.insertBeforeStep(multiQueryStep, position, traversal);
    }

    private void insertMultiQueryStepToTraversalEnd(Traversal.Admin traversal, boolean limitedBatch, int limitedBatchSize){
        Optional<Step> optionalRepeatEndMultiQueryPosition = JanusGraphTraversalUtil.getEndMultiQueryPosition(traversal);
        if(!optionalRepeatEndMultiQueryPosition.isPresent()){
            return;
        }

        Step currentStep = optionalRepeatEndMultiQueryPosition.get();
        insertMultiQueryStep(currentStep, limitedBatch, limitedBatchSize);
    }

    /**
     * Looks for MultiQueriables in within the traversal and registers them as clients of their respective
     * JanusGraphMultiQuerySteps
     *
     * @param traversal The local traversal layer.
     */
    private void configureMultiQueriables(final Admin<?, ?> traversal, boolean limitedBatch, int limitedBatchSize) {
        TraversalHelper.getStepsOfAssignableClass(MultiQueriable.class, traversal).forEach(multiQueriable -> {
            MultiQueryPositions multiQueryPositions = JanusGraphTraversalUtil.getAllMultiQueryPositionsForMultiQueriable(multiQueriable);
            boolean barrierSizeSet = !limitedBatch;
            boolean multiQueryUsed = false;
            if(multiQueryPositions.nextLoopMultiQueryStepLocation != null &&
                JanusGraphTraversalUtil.isLegalMultiQueryPosition(multiQueryPositions.nextLoopMultiQueryStepLocation)){
                // MultiQuery is applicable
                multiQueryUsed = true;
                if(applyPreviousMultiQueryAndReturnIfBarrierSizeSet(multiQueriable,
                    multiQueryPositions.nextLoopMultiQueryStepLocation, barrierSizeSet, true)){
                    barrierSizeSet = true;
                }
                multiQueriable.setUseMultiQuery(true);
            }

            // If one position is not legal, this means that the entire step can not use the multiQuery feature.
            for (Step mqPos : multiQueryPositions.currentLoopMultiQueryStepLocations) {
                if (!JanusGraphTraversalUtil.isLegalMultiQueryPosition(mqPos)) {
                    if(multiQueryUsed && !barrierSizeSet){
                        multiQueriable.setBatchSize(limitedBatchSize);
                    }
                    return;
                }
            }

            for (Step mqPos : multiQueryPositions.currentLoopMultiQueryStepLocations) {
                if(applyPreviousMultiQueryAndReturnIfBarrierSizeSet(multiQueriable, mqPos, barrierSizeSet, false)){
                    barrierSizeSet = true;
                }
            }

            if(!barrierSizeSet){
                multiQueriable.setBatchSize(limitedBatchSize);
            }

            if(!multiQueryPositions.currentLoopMultiQueryStepLocations.isEmpty()){
                // MultiQuery is applicable
                multiQueriable.setUseMultiQuery(true);
            }
        });
    }

    private boolean applyPreviousMultiQueryAndReturnIfBarrierSizeSet(MultiQueriable multiQueriableStep, Step multiQueryPositionStep, boolean barrierSizeSet, boolean nextLoop){
        final Optional<JanusGraphMultiQueryStep> optionalMultiQueryStep =
            JanusGraphTraversalUtil.getPreviousStepOfClass(JanusGraphMultiQueryStep.class, multiQueryPositionStep);
        if(optionalMultiQueryStep.isPresent()){
            JanusGraphMultiQueryStep multiQueryStep = optionalMultiQueryStep.get();
            if(nextLoop){
                multiQueryStep.attachNextLoopClient(multiQueriableStep);
            } else {
                multiQueryStep.attachSameLoopClient(multiQueriableStep);
            }
            if(!barrierSizeSet){
                Optional<Integer> optionalRelatedBarrierStepSize = multiQueryStep.getRelatedBarrierStepSize();
                if(optionalRelatedBarrierStepSize.isPresent()){
                    multiQueriableStep.setBatchSize(optionalRelatedBarrierStepSize.get());
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        return PRIORS;
    }

    public static JanusGraphMultiQueryStrategy instance() {
        return INSTANCE;
    }
}

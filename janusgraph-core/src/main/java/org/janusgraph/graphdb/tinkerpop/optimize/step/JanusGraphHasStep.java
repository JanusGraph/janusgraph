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

package org.janusgraph.graphdb.tinkerpop.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.graphdb.query.profile.QueryProfiler;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryBuilder;
import org.janusgraph.graphdb.query.vertex.BasicVertexCentricQueryUtil;
import org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher.FetchQueryBuildFunction;
import org.janusgraph.graphdb.tinkerpop.optimize.step.fetcher.HasStepBatchFetcher;
import org.janusgraph.graphdb.tinkerpop.profile.TP3ProfileWrapper;
import org.janusgraph.graphdb.types.system.ImplicitKey;
import org.janusgraph.graphdb.util.CopyStepUtil;
import org.janusgraph.graphdb.util.JanusGraphTraverserUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class extends the default TinkerPop's {@link  HasStep} and adds vertices multi-query optimization to this step.
 * <p>
 * Before this step is evaluated it usually receives multiple future vertices which might be processed next with this step.
 * This step stores all these vertices which might be needed later for evaluation and whenever this step receives the
 * vertex for evaluation which wasn't preFetched previously it sends multi-query for a batch of vertices to fetch their
 * properties and / or labels.
 * <p>
 * PreFetch logic in this step fetches either all properties or only selected properties (depending on how this step is configured)
 * for all registered vertices, but no more than `txVertexCacheSize`. If there are more registered vertices than `txVertexCacheSize`,
 * they will be left for later preFetch operation when these vertices become necessary.
 * <p>
 * This step optimizes only access to Vertex properties and skips optimization for any other Element.
 */
public class JanusGraphHasStep<S extends Element> extends HasStep<S> implements Profiling, MultiQueriable<S,S> {

    private boolean useMultiQuery = false;
    private int txVertexCacheSize = 20000;
    private final Set<String> propertiesToFetch = new HashSet<>();
    private final List<HasContainer> idHasContainers = new ArrayList<>();
    private final List<HasContainer> labelHasContainers = new ArrayList<>();
    private final List<HasContainer> propertyHasContainers = new ArrayList<>();
    private boolean prefetchAllPropertiesRequired;
    private QueryProfiler queryProfiler = QueryProfiler.NO_OP;
    private int batchSize = Integer.MAX_VALUE;
    private HasStepBatchFetcher hasStepBatchFetcher;

    public JanusGraphHasStep(Traversal.Admin traversal, HasContainer... hasContainers) {
        super(traversal, hasContainers);
        generatePrefetchRequirements();
    }

    public JanusGraphHasStep(HasStep<S> originalStep){
        this(originalStep.getTraversal(), originalStep.getHasContainers().toArray(new HasContainer[0]));
        CopyStepUtil.copyAbstractStepModifiableFields(originalStep, this);

        if (originalStep instanceof JanusGraphHasStep) {
            JanusGraphHasStep originalJanusGraphHasStep = (JanusGraphHasStep) originalStep;
            this.txVertexCacheSize = originalJanusGraphHasStep.txVertexCacheSize;
            setBatchSize(originalJanusGraphHasStep.batchSize);
            setUseMultiQuery(originalJanusGraphHasStep.useMultiQuery);
        }
    }

    @Override
    public void addHasContainer(final HasContainer hasContainer) {
        super.addHasContainer(hasContainer);
        withPrefetch(hasContainer);
    }

    @Override
    public void removeHasContainer(final HasContainer hasContainer) {
        super.removeHasContainer(hasContainer);
        propertiesToFetch.clear();
        labelHasContainers.clear();
        idHasContainers.clear();
        propertyHasContainers.clear();
        generatePrefetchRequirements();
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        if (useMultiQuery && traverser.get() instanceof Vertex) {
            return hasStepBatchFetcher.fetchData(getTraversal(), (Vertex) traverser.get(), JanusGraphTraverserUtil.getLoops(traverser));
        }
        return super.filter(traverser);
    }

    @Override
    public void setUseMultiQuery(boolean useMultiQuery) {
        this.useMultiQuery = useMultiQuery;
        if(useMultiQuery && hasStepBatchFetcher == null){
            hasStepBatchFetcher = new HasStepBatchFetcher(idHasContainers, labelHasContainers, propertyHasContainers, getFetcherBatchSize(),
                new FetchQueryBuildFunction() {
                    @Override
                    public <Q extends BaseVertexQuery> Q makeQuery(Q query) {
                        return (Q) BasicVertexCentricQueryUtil.withLabelVertices((BasicVertexCentricQueryBuilder) query)
                            .profiler(queryProfiler);
                    }
                },
                new FetchQueryBuildFunction() {
                    private String[] propertyKeys;
                    @Override
                    public <Q extends BaseVertexQuery> Q makeQuery(Q query) {
                        if(!prefetchAllPropertiesRequired){
                            if(propertyKeys == null){
                                propertyKeys = propertiesToFetch.toArray(new String[0]);
                            }
                            query.keys(propertyKeys);
                        }
                        return (Q) ((BasicVertexCentricQueryBuilder) query).profiler(queryProfiler);
                    }
                });
        }
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
        if(hasStepBatchFetcher != null){
            hasStepBatchFetcher.setBatchSize(getFetcherBatchSize());
        }
    }

    @Override
    public void registerFirstNewLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            hasStepBatchFetcher.registerFirstNewLoopFutureVertexForPrefetching(futureVertex);
        }
    }

    @Override
    public void registerSameLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            hasStepBatchFetcher.registerCurrentLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }

    @Override
    public void registerNextLoopFutureVertexForPrefetching(Vertex futureVertex, int futureVertexTraverserLoop) {
        if(useMultiQuery){
            hasStepBatchFetcher.registerNextLoopFutureVertexForPrefetching(futureVertex, futureVertexTraverserLoop);
        }
    }

    @Override
    public void setMetrics(MutableMetrics metrics) {
        queryProfiler = new TP3ProfileWrapper(metrics);
    }

    public void setTxVertexCacheSize(int txVertexCacheSize) {
        this.txVertexCacheSize = txVertexCacheSize;
        if(hasStepBatchFetcher != null){
            hasStepBatchFetcher.setBatchSize(getFetcherBatchSize());
        }
    }

    private void generatePrefetchRequirements(){
        for (final HasContainer condition : getHasContainers()) {
            withPrefetch(condition);
        }
    }

    private void withPrefetch(HasContainer condition){
        if(ImplicitKey.ID.name().equals(condition.getKey())){
            idHasContainers.add(condition);
        } else if(ImplicitKey.LABEL.name().equals(condition.getKey())){
            labelHasContainers.add(condition);
        } else {
            propertyHasContainers.add(condition);
            propertiesToFetch.add(condition.getKey());
        }
    }

    public void withPropertyPrefetch(String key){
        propertiesToFetch.add(key);
    }

    public boolean isPrefetchAllPropertiesRequired() {
        return prefetchAllPropertiesRequired;
    }

    public void setPrefetchAllPropertiesRequired(boolean prefetchAllPropertiesRequired) {
        this.prefetchAllPropertiesRequired = prefetchAllPropertiesRequired;
    }

    private int getFetcherBatchSize(){
        return Math.min(batchSize, txVertexCacheSize);
    }
}

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

package org.janusgraph.graphdb.tinkerpop.optimize.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.graphdb.util.JanusGraphTraverserUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

/**
 * This step can be injected before a traversal parent, such as a union, and will cache the
 * starts sent to the parent. The traversal parent will drip feed those starts into its
 * child traversal. If the initial step of that child supports multiQuery then its faster
 * if initialised with all the starts than just one at a time, so this step allows it to
 * request the full set of starts from this step when initialising itself.
 */
public final class JanusGraphMultiQueryStep extends AbstractStep<Element, Element> {

    /**
     * All steps that use this step to fill their cache. For example, this could be the
     * next JanusGraphVertexStep. If the next step is a MultiQuery compatible parent
     * (such as union()), then all of its child traversals can use this cache. Thus,
     * there can be more than one client step.
     */
    private List<MultiQueriable> firstLoopClientSteps = new ArrayList<>();
    private List<MultiQueriable> sameLoopClientSteps = new ArrayList<>();
    private List<MultiQueriable> nextLoopClientSteps = new ArrayList<>();

    private boolean initialized;
    private boolean limitBatchSize;
    private NoOpBarrierStep generatedBarrierStep;
    private Integer relatedBarrierStepSize;

    public JanusGraphMultiQueryStep(Traversal.Admin traversal, boolean limitBatchSize) {
        this(traversal, limitBatchSize, null, null);
    }

    public JanusGraphMultiQueryStep(Traversal.Admin traversal, boolean limitBatchSize, NoOpBarrierStep generatedBarrierStep) {
        this(traversal, limitBatchSize, generatedBarrierStep, generatedBarrierStep.getMaxBarrierSize());
    }

    public JanusGraphMultiQueryStep(Traversal.Admin traversal, boolean limitBatchSize, Integer relatedBarrierStepSize) {
        this(traversal, limitBatchSize, null, relatedBarrierStepSize);
    }

    private JanusGraphMultiQueryStep(Traversal.Admin traversal, boolean limitBatchSize, NoOpBarrierStep generatedBarrierStep, Integer relatedBarrierStepSize) {
        super(traversal);
        this.limitBatchSize = limitBatchSize;
        this.initialized = false;
        this.generatedBarrierStep = generatedBarrierStep;
        this.relatedBarrierStepSize = relatedBarrierStepSize;
    }

    public void attachFirstLoopClient(MultiQueriable mq) {
        firstLoopClientSteps.add(mq);
    }

    public void attachSameLoopClient(MultiQueriable mq) {
        sameLoopClientSteps.add(mq);
    }

    public void attachNextLoopClient(MultiQueriable mq) {
        nextLoopClientSteps.add(mq);
    }

    private void initialize() {
        assert !initialized;
        initialized = true;

        if (!limitBatchSize && (!sameLoopClientSteps.isEmpty() || !nextLoopClientSteps.isEmpty() || !firstLoopClientSteps.isEmpty())) { // eagerly cache all starts instead of batching
            if (!starts.hasNext()) {
                throw FastNoSuchElementException.instance();
            }
            final List<Traverser.Admin<Element>> elements = new ArrayList<>();
            starts.forEachRemaining(e -> {
                elements.add(e);
                registerTraverser(e);
            });
            starts.add(elements.iterator());
        }
    }

    @Override
    protected Admin<Element> processNextStart() throws NoSuchElementException {
        if (!initialized) {
            initialize();
        }
        Admin<Element> start = this.starts.next();
        registerTraverser(start);
        return start;
    }

    private void registerTraverser(Admin<Element> traverser){
        if (traverser.get() instanceof Vertex) {
            Vertex vertex = (Vertex) traverser.get();
            int loops = JanusGraphTraverserUtil.getLoops(traverser);
            firstLoopClientSteps.forEach(client -> client.registerFirstNewLoopFutureVertexForPrefetching(vertex, loops));
            sameLoopClientSteps.forEach(client -> client.registerSameLoopFutureVertexForPrefetching(vertex, loops));
            nextLoopClientSteps.forEach(client -> client.registerNextLoopFutureVertexForPrefetching(vertex, loops));
        }
    }

    @Override
    public JanusGraphMultiQueryStep clone() {
        JanusGraphMultiQueryStep clone = (JanusGraphMultiQueryStep) super.clone();
        clone.sameLoopClientSteps = new ArrayList<>(sameLoopClientSteps);
        clone.nextLoopClientSteps = new ArrayList<>(nextLoopClientSteps);
        clone.firstLoopClientSteps = new ArrayList<>(firstLoopClientSteps);
        clone.limitBatchSize = limitBatchSize;
        clone.relatedBarrierStepSize = relatedBarrierStepSize;
        if(generatedBarrierStep != null){
            clone.generatedBarrierStep = generatedBarrierStep.clone();
        }
        clone.initialized = false;
        return clone;
    }

    @Override
    public void reset() {
        super.reset();
        this.initialized = false;
    }

    public boolean isLimitBatchSize() {
        return limitBatchSize;
    }

    public List<MultiQueriable> getFirstLoopClientSteps() {
        return Collections.unmodifiableList(firstLoopClientSteps);
    }

    public List<MultiQueriable> getSameLoopClientSteps() {
        return Collections.unmodifiableList(sameLoopClientSteps);
    }

    public List<MultiQueriable> getNextLoopClientSteps() {
        return Collections.unmodifiableList(nextLoopClientSteps);
    }

    public boolean isFirstLoopClientStepsEmpty() {
        return firstLoopClientSteps.isEmpty();
    }

    public boolean isSameLoopClientStepsEmpty() {
        return sameLoopClientSteps.isEmpty();
    }

    public boolean isNextLoopClientStepsEmpty() {
        return nextLoopClientSteps.isEmpty();
    }

    public NoOpBarrierStep getGeneratedBarrierStep() {
        return generatedBarrierStep;
    }

    public Optional<Integer> getRelatedBarrierStepSize() {
        return Optional.ofNullable(relatedBarrierStepSize);
    }
}

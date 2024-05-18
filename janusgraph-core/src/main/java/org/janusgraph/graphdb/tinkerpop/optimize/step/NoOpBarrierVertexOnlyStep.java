// Copyright 2024 JanusGraph Authors
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
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * This is implementation of the standard NoOpBarrierStep with the same logic except that anytime we receive non-Vertex
 * element during `processAllStarts` process we stop barrier fetching more data into the barrier until that non-Vertex
 * element is out from this barrier step.
 * It means that for non-Vertex elements this barrier acts like it has size of `1`. For Vertex elements it acts
 * same as normal `NoOpBarrierStep`.
 * TODO: Instead of reimplementing this logic, we should simple extend `NoOpBarrierStep` and overwrite `processAllStarts`
 * method only. This should be possible after the following PR is part of the next TinkerPop release:
 * <a href="https://github.com/apache/tinkerpop/pull/2612">PR #2612</a>
 * For information regarding why this barrier step implementation is needed see discussion here:
 * <a href="https://github.com/JanusGraph/janusgraph/pull/4456">PR #4456</a> <br>
 * When refactoring ensure to remove `instanceof NoOpBarrierVertexOnlyStep` everywhere from JanusGraph code.
 */
public class NoOpBarrierVertexOnlyStep<S> extends AbstractStep<S, S> implements LocalBarrier<S> {

    protected int maxBarrierSize;
    protected TraverserSet<S> barrier;

    public NoOpBarrierVertexOnlyStep(final Traversal.Admin traversal) {
        this(traversal, Integer.MAX_VALUE);
    }

    public NoOpBarrierVertexOnlyStep(final Traversal.Admin traversal, final int maxBarrierSize) {
        this(traversal, maxBarrierSize, (TraverserSet<S>) traversal.getTraverserSetSupplier().get());
    }

    public NoOpBarrierVertexOnlyStep(final Traversal.Admin traversal, final int maxBarrierSize, TraverserSet<S> barrier) {
        super(traversal);
        this.maxBarrierSize = maxBarrierSize;
        this.barrier = barrier;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.barrier.isEmpty())
            this.processAllStarts();
        return this.barrier.remove();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.BULK);
    }

    @Override
    public void processAllStarts() {
        if(!this.barrier.isEmpty()){
            return;
        }
        while ((this.maxBarrierSize == Integer.MAX_VALUE || this.barrier.size() < this.maxBarrierSize) && this.starts.hasNext()) {
            final Traverser.Admin<S> traverser = this.starts.next();
            traverser.setStepId(this.getNextStep().getId()); // when barrier is reloaded, the traversers should be at the next step
            this.barrier.add(traverser);
            if(!(traverser.get() instanceof Vertex)){
                break;
            }
        }
    }

    @Override
    public boolean hasNextBarrier() {
        this.processAllStarts();
        return !this.barrier.isEmpty();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        this.processAllStarts();
        if (this.barrier.isEmpty())
            throw FastNoSuchElementException.instance();
        else {
            final TraverserSet<S> temp = this.barrier;
            this.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
            return temp;
        }
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        this.barrier.addAll(barrier);
    }

    @Override
    public NoOpBarrierVertexOnlyStep<S> clone() {
        final NoOpBarrierVertexOnlyStep<S> clone = (NoOpBarrierVertexOnlyStep<S>) super.clone();
        clone.barrier = (TraverserSet<S>) this.traversal.getTraverserSetSupplier().get();
        return clone;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.maxBarrierSize == Integer.MAX_VALUE ? null : this.maxBarrierSize);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.maxBarrierSize;
    }

    @Override
    public void reset() {
        super.reset();
        this.barrier.clear();
    }

    public int getMaxBarrierSize() {
        return maxBarrierSize;
    }
}

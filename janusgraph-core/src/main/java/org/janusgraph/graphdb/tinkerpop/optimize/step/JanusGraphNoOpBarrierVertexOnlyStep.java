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
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * This implementation overwrites the standard NoOpBarrierStep with the same logic except that anytime we receive non-Vertex
 * element during `processAllStarts` process we stop barrier fetching of more data into the barrier until that non-Vertex
 * element is out from this barrier step.
 * It means that for non-Vertex elements this barrier acts like it has size of `1`. For Vertex elements it acts
 * same as normal `NoOpBarrierStep`.
 */
public class JanusGraphNoOpBarrierVertexOnlyStep<S> extends NoOpBarrierStep<S> {

    public JanusGraphNoOpBarrierVertexOnlyStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    public JanusGraphNoOpBarrierVertexOnlyStep(final Traversal.Admin traversal, final int maxBarrierSize) {
        super(traversal, maxBarrierSize);
    }

    public JanusGraphNoOpBarrierVertexOnlyStep(final Traversal.Admin traversal, final int maxBarrierSize, TraverserSet<S> barrier) {
        super(traversal, maxBarrierSize, barrier);
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
    public JanusGraphNoOpBarrierVertexOnlyStep<S> clone() {
        return (JanusGraphNoOpBarrierVertexOnlyStep<S>) super.clone();
    }

}

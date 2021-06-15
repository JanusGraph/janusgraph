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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

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
    private List<MultiQueriable> clientSteps = new ArrayList<>();
    private final boolean limitBatchSize;
    private boolean initialized;

    public JanusGraphMultiQueryStep(Traversal.Admin traversal, boolean limitBatchSize) {
        super(traversal);
        this.limitBatchSize = limitBatchSize;
        this.initialized = false;
    }

    public void attachClient(MultiQueriable mq) {
        clientSteps.add(mq);
    }

    private void initialize() {
        assert !initialized;
        initialized = true;

        if (!limitBatchSize && !clientSteps.isEmpty()) { // eagerly cache all starts instead of batching
            if (!starts.hasNext()) {
                throw FastNoSuchElementException.instance();
            }
            final List<Traverser.Admin<Element>> elements = new ArrayList<>();
            starts.forEachRemaining(e -> {
                elements.add(e);
                if (e.get() instanceof Vertex) {
                    clientSteps.forEach(client -> client.registerFutureVertexForPrefetching((Vertex) e.get()));
                }
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
        if (start.get() instanceof Vertex) {
            clientSteps.forEach(client -> client.registerFutureVertexForPrefetching((Vertex) start.get()));
        }
        return start;
    }

    @Override
    public JanusGraphMultiQueryStep clone() {
        JanusGraphMultiQueryStep clone = (JanusGraphMultiQueryStep) super.clone();
        clone.clientSteps = new ArrayList<>(clientSteps);
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

    public List<MultiQueriable> getClientSteps() {
        return Collections.unmodifiableList(clientSteps);
    }
}

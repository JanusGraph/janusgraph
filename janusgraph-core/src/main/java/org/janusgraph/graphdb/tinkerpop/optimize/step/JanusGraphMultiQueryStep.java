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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * This step can be injected before a traversal parent, such as a union, and will cache the
 * starts sent to the parent. The traversal parent will drip feed those starts into its
 * child traversal. If the initial step of that child supports multiQuery then its faster
 * if initialised with all the starts than just one at a time, so this step allows it to
 * request the full set of starts from this step when initialising itself.
 */
public final class JanusGraphMultiQueryStep extends AbstractStep<Vertex, Vertex> {

    private final Set<Traverser.Admin<Vertex>> cachedStarts = new HashSet<Traverser.Admin<Vertex>>();
    private final String forStep;
    private boolean cachedStartsAccessed = false;

    public JanusGraphMultiQueryStep(Step<Vertex,?> originalStep) {
        super(originalStep.getTraversal());
        this.forStep = originalStep.getClass().getSimpleName();
    }

    @Override
    protected Admin<Vertex> processNextStart() throws NoSuchElementException {
        Admin<Vertex> start = this.starts.next();
        if (!cachedStarts.contains(start))
        {
            if (cachedStartsAccessed) {
                cachedStarts.clear();
                cachedStartsAccessed = false;
            }
            final List<Traverser.Admin<Vertex>> newStarters = new ArrayList<>();
            starts.forEachRemaining(s -> {
                newStarters.add(s);
                cachedStarts.add(s);
            });
            starts.add(newStarters.iterator());
            cachedStarts.add(start);
        }
        return start;
    }

    public List<Traverser.Admin<Vertex>> getCachedStarts() {
        cachedStartsAccessed = true;
        return new ArrayList<>(cachedStarts);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, forStep);
    }

    @Override
    public void reset() {
        super.reset();
        this.cachedStarts.clear();
    }
}

// Copyright 2026 JanusGraph Authors
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

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A {@link RepeatStep} which does not let the barrier steps inserted by JanusGraph's multi-query optimization
 * ({@link JanusGraphNoOpBarrierVertexOnlyStep}) change the semantics of the {@code repeat()} step.
 * <p>
 * Since TinkerPop 3.8 a {@link RepeatStep} whose repeat traversal contains a {@link Barrier} adds all of its start
 * traversers to the repeat traversal before the first iteration (the repeat traversal is treated as a "global" child),
 * while a repeat traversal without barriers is fed one start traverser at a time. JanusGraph's limited batch
 * multi-query inserts {@link JanusGraphNoOpBarrierVertexOnlyStep}s into repeat traversals, which would switch every
 * {@code repeat()} into the global mode and thereby keep all start traversers of the repeat in memory instead of
 * processing them in bounded batches. This step therefore ignores the barriers inserted by JanusGraph when deciding
 * between the two modes, so that {@code repeat()} behaves exactly like TinkerPop's {@link RepeatStep} would for the
 * traversal as written by the user.
 */
public class JanusGraphRepeatStep<S> extends RepeatStep<S> {

    private Boolean containsNonJanusGraphBarrier = null;

    public JanusGraphRepeatStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    /**
     * Replaces the given {@link RepeatStep} of the given traversal with an equivalent {@link JanusGraphRepeatStep}.
     *
     * @return the step which replaced {@code repeatStep}
     */
    public static <S> JanusGraphRepeatStep<S> replace(final RepeatStep<S> repeatStep, final Traversal.Admin<?, ?> traversal) {
        final JanusGraphRepeatStep<S> janusGraphRepeatStep = new JanusGraphRepeatStep<>(traversal);
        final Traversal.Admin<S, S> repeatTraversal = repeatStep.getRepeatTraversal();
        if (repeatTraversal != null) {
            // setRepeatTraversal() appends its own RepeatEndStep, so drop the one appended by the replaced step first
            final Step<?, ?> endStep = repeatTraversal.getEndStep();
            if (endStep instanceof RepeatEndStep) {
                repeatTraversal.removeStep(endStep);
            }
            janusGraphRepeatStep.setRepeatTraversal(repeatTraversal);
        }
        if (repeatStep.getUntilTraversal() != null) {
            janusGraphRepeatStep.setUntilTraversal(repeatStep.getUntilTraversal());
        }
        if (repeatStep.getEmitTraversal() != null) {
            janusGraphRepeatStep.setEmitTraversal(repeatStep.getEmitTraversal());
        }
        // the setters above derive untilFirst/emitFirst from the order in which they are called, restore the originals
        janusGraphRepeatStep.untilFirst = repeatStep.untilFirst;
        janusGraphRepeatStep.emitFirst = repeatStep.emitFirst;
        janusGraphRepeatStep.setLoopName(repeatStep.getLoopName());
        janusGraphRepeatStep.setId(repeatStep.getId());
        TraversalHelper.copyLabels(repeatStep, janusGraphRepeatStep, false);
        TraversalHelper.replaceStep(repeatStep, janusGraphRepeatStep, traversal);
        return janusGraphRepeatStep;
    }

    @Override
    protected Iterator<Traverser.Admin<S>> standardAlgorithm() throws NoSuchElementException {
        final Traversal.Admin<S, S> repeatTraversal = getRepeatTraversal();
        if (null == repeatTraversal) {
            throw new IllegalStateException("The repeat()-traversal was not defined: " + this);
        }
        if (containsNonJanusGraphBarrier()) {
            // the repeat traversal contains a barrier which was not inserted by JanusGraph: follow TinkerPop's
            // semantics of a repeat traversal with barriers and add all start traversers before iterating
            return super.standardAlgorithm();
        }
        // TinkerPop's semantics of a repeat traversal without barriers: feed the start traversers one at a time
        while (true) {
            if (repeatTraversal.getEndStep().hasNext()) {
                return repeatTraversal.getEndStep();
            } else {
                final Traverser.Admin<S> start = this.starts.next();
                start.initialiseLoops(this.getId(), this.getLoopName());
                if (doUntil(start, true)) {
                    start.resetLoops();
                    return IteratorUtils.of(start);
                }
                repeatTraversal.addStart(start);
                if (doEmit(start, true)) {
                    final Traverser.Admin<S> emitSplit = start.split();
                    emitSplit.resetLoops();
                    return IteratorUtils.of(emitSplit);
                }
            }
        }
    }

    private boolean containsNonJanusGraphBarrier() {
        if (containsNonJanusGraphBarrier == null) {
            containsNonJanusGraphBarrier = containsNonJanusGraphBarrier(getRepeatTraversal());
        }
        return containsNonJanusGraphBarrier;
    }

    private static boolean containsNonJanusGraphBarrier(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            if (step instanceof Barrier && !(step instanceof JanusGraphNoOpBarrierVertexOnlyStep)) {
                return true;
            }
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> child : ((TraversalParent) step).getLocalChildren()) {
                    if (containsNonJanusGraphBarrier(child)) {
                        return true;
                    }
                }
                for (final Traversal.Admin<?, ?> child : ((TraversalParent) step).getGlobalChildren()) {
                    if (containsNonJanusGraphBarrier(child)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public JanusGraphRepeatStep<S> clone() {
        final JanusGraphRepeatStep<S> clone = (JanusGraphRepeatStep<S>) super.clone();
        clone.containsNonJanusGraphBarrier = null;
        return clone;
    }
}

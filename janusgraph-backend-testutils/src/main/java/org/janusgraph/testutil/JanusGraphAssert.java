// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.testutil;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.StartStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.graphdb.query.profile.QueryProfiler;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphAssert {

    public static void assertCount(int expected, Traversal traversal) {
        assertEquals(expected, traversal.toList().size());
    }

    public static void assertCount(int expected, Collection collection) {
        assertEquals(expected, collection.size());
    }

    public static void assertCount(int expected, Iterable iterable) {
        assertEquals(expected, Iterables.size(iterable));
    }

    public static void assertCount(int expected, Iterator iterator) {
        assertEquals(expected, Iterators.size(iterator));
    }

    public static void assertCount(long expected, Stream stream) {
        assertEquals(expected, stream.count());
    }

    public static<V extends Element> void assertEmpty(Object object) {
        assertTrue(isEmpty(object));
    }

    public static<V extends Element> void assertNotEmpty(Object object) {
        assertFalse(isEmpty(object));
    }

    public static<E extends Element> void assertTraversal(GraphTraversal<?, E> req, E... expectedElements) {
        for (final E expectedElement : expectedElements) {
            assertEquals(expectedElement, req.next());
        }
        assertFalse(req.hasNext());
    }

    public static void assertIntRange(GraphTraversal<?, Integer> traversal, int start, int end) {
        int[] intArray;
        if (start <= end) {
            intArray = IntStream.range(start, end).toArray();
        } else {
            intArray = IntStream.range(end, start).map(i -> start + end - i).toArray();
        }
        assertArrayEquals(intArray, traversal.toList().stream().mapToInt(i -> i).toArray());
    }

    private static boolean hasBackendHit(Metrics metrics) {
        if (QueryProfiler.BACKEND_QUERY.equals(metrics.getName())) return true;
        for (Metrics subMetrics : metrics.getNested()) {
            if (hasBackendHit(subMetrics)) return true;
        }
        return false;
    }

    public static void assertBackendHit(TraversalMetrics profile) {
        assertTrue(profile.getMetrics().stream().anyMatch(JanusGraphAssert::hasBackendHit));
    }

    public static void assertNoBackendHit(TraversalMetrics profile) {
        assertFalse(profile.getMetrics().stream().anyMatch(JanusGraphAssert::hasBackendHit));
    }

    private static boolean isEmpty(Object obj) {
        Preconditions.checkArgument(obj != null);
        if (obj instanceof Traversal) return !((Traversal) obj).hasNext();
        else if (obj instanceof Collection) return ((Collection)obj).isEmpty();
        else if (obj instanceof Iterable) return Iterables.isEmpty((Iterable)obj);
        else if (obj instanceof Iterator) return !((Iterator)obj).hasNext();
        else if (obj instanceof Stream) return ((Stream) obj).count() == 0;
        else if (obj.getClass().isArray()) return Array.getLength(obj)==0;
        throw new IllegalArgumentException("Cannot determine size of: " + obj);
    }

    /**
     * Checks the number of matching steps within a traversal and the size of it's evaluated output simultaneously.
     *
     * @param expectedResults The expected number of returned results.
     * @param expectedSteps The expected number of steps of type <code>expectedStepTypes</code>.
     * @param traversal The checked traversal.
     * @param expectedStepTypes The step types to be counted.
     */
    public static void assertNumStep(int expectedResults, int expectedSteps, GraphTraversal traversal, Class<? extends Step>... expectedStepTypes) {
        assertEquals(expectedResults, traversal.toList().size());

        //Verify that steps line up with what is expected after JanusGraph's optimizations are applied
        List<Step> steps = traversal.asAdmin().getSteps();
        Set<Class<? extends Step>> expSteps = Sets.newHashSet(expectedStepTypes);
        int numSteps = 0;
        for (Step s : steps) {
            if (s.getClass().equals(GraphStep.class) || s.getClass().equals(StartStep.class)) continue;

            if (expSteps.contains(s.getClass())) {
                numSteps++;
            }
        }
        assertEquals(expectedSteps, numSteps);
    }

    public static void assertOptimization(Traversal<?, ?> expectedTraversal, Traversal<?, ?> originalTraversal,
                                          TraversalStrategy... optimizationStrategies) {
        final TraversalStrategies optimizations = new DefaultTraversalStrategies();
        for (final TraversalStrategy<?> strategy : optimizationStrategies) {
            optimizations.addStrategies(strategy);
        }

        originalTraversal.asAdmin().setStrategies(optimizations);
        originalTraversal.asAdmin().applyStrategies();

        assertEquals(expectedTraversal.asAdmin().getSteps().toString(),
            originalTraversal.asAdmin().getSteps().toString());
    }

    public static void assertSameResultWithOptimizations(Traversal<?, ?> originalTraversal, TraversalStrategy<?>... strategies) {
        Traversal.Admin<?,?> optimizedTraversal = originalTraversal.asAdmin().clone();
        optimizedTraversal.getStrategies().addStrategies(strategies);
        List<?> optimizedResult = optimizedTraversal.toList();

        Traversal.Admin<?,?> unOptimizedTraversal = originalTraversal.asAdmin().clone();
        Stream.of(strategies).forEach(s -> unOptimizedTraversal.getStrategies().removeStrategies(s.getClass()));
        List<?> unOptimizedResult = unOptimizedTraversal.toList();

        assertEquals(unOptimizedResult, optimizedResult);
    }

    public static boolean queryProfilerAnnotationIsPresent(Traversal t, String queryProfilerAnnotation) {
        TraversalMetrics metrics = t.asAdmin().getSideEffects().get("~metrics");
        return metrics.toString().contains(queryProfilerAnnotation + "=true");
    }

    public static void assertContains(Metrics metrics, String annotationKey, Object annotationValue){
        Map<String, Object> annotations = metrics.getAnnotations();
        assertTrue(annotations.containsKey(annotationKey));
        assertEquals(annotationValue, annotations.get(annotationKey));
    }

    public static void assertNotContains(Metrics metrics, String annotationKey, Object annotationValue){
        Map<String, Object> annotations = metrics.getAnnotations();
        if(annotations.containsKey(annotationKey)){
            assertNotEquals(annotationValue, annotations.get(annotationKey));
        }
    }

    public static Metrics getStepMetrics(TraversalMetrics traversalMetrics, Class<? extends Step> stepClass){
        String stepMetricsName = stepClass.getSimpleName();
        for(Metrics metrics : traversalMetrics.getMetrics()){
            if(metrics.getName().startsWith(stepMetricsName)){
                return metrics;
            }
        }
        return null;
    }
}

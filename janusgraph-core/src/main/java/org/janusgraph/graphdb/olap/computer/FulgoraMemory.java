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

package org.janusgraph.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.janusgraph.core.JanusGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraMemory implements Memory.Admin {

    private static final Logger log = LoggerFactory.getLogger(FulgoraMemory.class);

    public final Map<String, MemoryComputeKey> memoryKeys = new HashMap<>();
    public Map<String, Optional<Object>> previousMap;
    public final Map<String, Optional<Object>> currentMap;
    //The values which the workers of an iteration add to the broadcast keys whose reducers may change their first
    //argument, reduced with each other until the workers have ended, see add()
    private final Map<String, Optional<Object>> broadcastAdditions = new ConcurrentHashMap<>();
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0L);
    private volatile boolean inExecute = false;

    public FulgoraMemory(final VertexProgram<?> vertexProgram, final Set<MapReduce> mapReducers) {
        this.currentMap = new ConcurrentHashMap<>();
        this.previousMap = new ConcurrentHashMap<>();
        if (null != vertexProgram) {
            for (final MemoryComputeKey key : vertexProgram.getMemoryComputeKeys()) {
                this.memoryKeys.put(key.getKey(), key);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.memoryKeys.put(mapReduce.getMemoryKey(), MemoryComputeKey.of(mapReduce.getMemoryKey(), Operator.assign, false, false));
        }
    }

    @Override
    public Set<String> keys() {
        return this.previousMap.keySet().stream().filter(key -> !this.inExecute || this.memoryKeys.get(key).isBroadcast()).collect(Collectors.toSet());
    }

    @Override
    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    protected void complete() {
        this.iteration.decrementAndGet();
        this.previousMap = this.currentMap;
        this.memoryKeys.values().stream().filter(MemoryComputeKey::isTransient).forEach(computeKey -> this.previousMap.remove(computeKey.getKey()));
    }

    protected void completeSubRound() {
        if (this.inExecute) {
            //The iteration's workers have ended, so the values they added to the broadcast keys join the keys' values.
            //Each key's additions are taken out as they join, so that one which a worker made late joins at the next
            //iteration's end, or never where none follows
            for (String key : new ArrayList<>(this.broadcastAdditions.keySet())) {
                final Optional<Object> added = this.broadcastAdditions.remove(key);
                try {
                    join(key, added.orElse(null));
                } catch (Exception e) {
                    //Numbered from 1, as FulgoraGraphComputer numbers the iterations which fail
                    throw new JanusGraphException("Could not reduce the values added to memory key [" + key +
                        "] in vertex program iteration [" + (getIteration() + 1) + "] into its value", e);
                }
            }
        }
        this.previousMap = new ConcurrentHashMap<>(this.currentMap);
        this.inExecute = !this.inExecute;
    }

    @Override
    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        if (!this.previousMap.containsKey(key)) throw Memory.Exceptions.memoryDoesNotExist(key);

        final Optional<Object> o = this.previousMap.get(key);
        final R r = (R) o.orElse(null);
        if (this.inExecute && !this.memoryKeys.get(key).isBroadcast())
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    /**
     * Adds the value to the key with the key's reducer. The workers of an iteration read the value of a broadcast key
     * as the iteration before left it, while other workers add to the key, and a reducer may change its first
     * argument, as {@link Operator#addAll} does with the collection of an {@code aggregate()} step. Reduced into the
     * key's value at once, the value added would change a collection which other workers are reading. So the values
     * added to a broadcast key whose reducer may change its first argument, {@code addAll} or a reducer which isn't
     * one of TinkerPop's operators, are reduced with each other, into the first one, like TinkerGraphComputer and
     * SparkGraphComputer, which reduce each worker's (task's) additions into the first one. Their result is reduced
     * into the key's value once the workers have ended. The first value added thus decides how the iteration's
     * additions combine, so they have to be mutable and of the key's own kind, and a reducer of one's own has to be
     * associative and take two additions. That holds for the side effects of a traversal too: with a reducer of its
     * own, given to {@code withSideEffect(key, value, reducer)}, {@code aggregate()} adds each element on its own, and
     * so does a step which calls {@code Traverser.sideEffects(key, value)}, so the reducer combines two elements, where
     * {@code addAll}, given a collection to start from, collects them. The values added with TinkerPop's other
     * operators, which change neither argument, and those added to the keys which the workers can't read, are reduced
     * into the keys' values at once.
     */
    @Override
    public void add(final String key, final Object value) {
        checkKey(key);
        if (!this.inExecute && ("incr".equals(key) || "and".equals(key) || "or".equals(key)))
            throw Memory.Exceptions.memoryIsCurrentlyImmutable();
        else if (!this.inExecute)
            throw Memory.Exceptions.memoryAddOnlyDuringVertexProgramExecute(key);
        final MemoryComputeKey computeKey = this.memoryKeys.get(key);
        final boolean deferred = computeKey.isBroadcast() && mayChangeFirstArgument(computeKey.getReducer());
        reduce(deferred ? this.broadcastAdditions : this.currentMap, key, value);
    }

    //TinkerPop's operators other than addAll return a new value, or one of their arguments, and change neither
    private static boolean mayChangeFirstArgument(final BinaryOperator<?> reducer) {
        return !(reducer instanceof Operator) || reducer == Operator.addAll;
    }

    /*
     * Reduces the values which the workers added to the broadcast key in an iteration into the key's value. Where the
     * value is an empty BulkSet which addAll reduces into, as aggregate() starts from, the additions, a BulkSet as
     * well, take its place instead, rather than be held and inserted twice: the reduction would give a BulkSet equal
     * to them.
     */
    private void join(final String key, final Object added) {
        final BinaryOperator<Object> reducer = this.memoryKeys.get(key).getReducer();
        this.currentMap.compute(key, (k, v) -> {
            if (null == v || !v.isPresent()) {
                return Optional.ofNullable(added);
            } else if (reducer == Operator.addAll && v.get() instanceof BulkSet && ((BulkSet<?>) v.get()).isEmpty() &&
                    added instanceof BulkSet) {
                return Optional.of(added);
            }
            return Optional.ofNullable(reducer.apply(v.get(), added));
        });
    }

    private void reduce(final Map<String, Optional<Object>> values, final String key, final Object value) {
        values.compute(key, (k, v) -> Optional.ofNullable(null == v || !v.isPresent() ? value :
            this.memoryKeys.get(key).getReducer().apply(v.get(), value)));
    }

    @Override
    public void set(final String key, final Object value) {
        checkKey(key);
        if (this.inExecute)
            throw Memory.Exceptions.memorySetOnlyDuringVertexProgramSetUpAndTerminate(key);
        this.currentMap.put(key, Optional.ofNullable(value));
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    private void checkKey(final String key) {
        if (!this.memoryKeys.containsKey(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
    }

    /**
     * Detaches the elements among the values of the completed memory from the transactions of the job's scans, which
     * end with the job, so that the values stay usable afterwards: the side effects of a traversal collect the
     * vertices and edges which its scans read, as {@code group()} or {@code aggregate()} do, and a later job of the
     * traversal, or the user, reads them once the scans have ended. The elements are detached with their properties,
     * as {@link DetachedFactory#detach(Object, boolean)} does for the values of the traversers which reach the master
     * traversal, and the lists, sets and maps around them are rebuilt as the kinds that method makes. A value which
     * holds a vertex adjacent to a scanned one stays as it is: neither the label nor the properties of such a vertex
     * can be read in an OLAP job. The traversers of the program are left alone: it detaches the halted ones itself,
     * and {@link #attachReferenceElements(Graph)} attaches theirs to the graph.
     */
    protected void detachElements() {
        for (Map.Entry<String, Optional<Object>> entry : previousMap.entrySet()) {
            final Object value = entry.getValue().orElse(null);
            if (value == null || value instanceof TraverserSet) {
                continue;
            }
            try {
                final Object detached = DetachedFactory.detach(value, true);
                if (detached != value) {
                    entry.setValue(Optional.of(detached));
                }
            } catch (RuntimeException e) {
                log.debug("The elements in the value of memory key [{}] can't be detached and stay as they are",
                    entry.getKey(), e);
            }
        }
    }

    protected void attachReferenceElements(Graph graph) {
        currentMap.values().stream().filter(o -> o.isPresent()).map(o -> o.get()).filter(v -> v instanceof TraverserSet)
            .forEach(v -> attachReferenceElements((TraverserSet<Object>) v, graph));
    }

    private static void attachReferenceElements(TraverserSet<Object> toProcessTraversers, Graph graph) {
        toProcessTraversers.forEach(traverser -> {
            Object value = traverser.get();
            if (value instanceof ReferenceVertex) {
                Vertex vertex = ((ReferenceVertex) value).attach(Attachable.Method.get(graph));
                traverser.set(vertex);
            } else if (value instanceof ReferenceEdge) {
                Edge edge = ((ReferenceEdge) value).attach(Attachable.Method.get(graph));
                traverser.set(edge);
            }
        });
    }

}

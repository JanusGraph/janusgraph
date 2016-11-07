package com.thinkaurelius.titan.graphdb.olap.computer;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraMemory implements Memory.Admin {

    public final Map<String, MemoryComputeKey> memoryKeys = new HashMap<>();
    public Map<String, Object> previousMap;
    public Map<String, Object> currentMap;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);
    private boolean inExecute = false;

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
        this.previousMap = new ConcurrentHashMap<>(this.currentMap);
        this.inExecute = !this.inExecute;
    }

    @Override
    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        final R r = (R) this.previousMap.get(key);
        if (null == r)
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else if (this.inExecute && !this.memoryKeys.get(key).isBroadcast())
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    @Override
    public void add(final String key, final Object value) {
        checkKeyValue(key, value);
        if (!this.inExecute && ("incr".equals(key) || "and".equals(key) || "or".equals(key)))
            throw Memory.Exceptions.memoryIsCurrentlyImmutable();
        else if (!this.inExecute)
            throw Memory.Exceptions.memoryAddOnlyDuringVertexProgramExecute(key);
        this.currentMap.compute(key, (k, v) -> null == v ? value : this.memoryKeys.get(key).getReducer().apply(v, value));
    }

    @Override
    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        if (this.inExecute)
            throw Memory.Exceptions.memorySetOnlyDuringVertexProgramSetUpAndTerminate(key);
        this.currentMap.put(key, value);
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    private void checkKeyValue(final String key, final Object value) {
        if (!this.memoryKeys.containsKey(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
        MemoryHelper.validateValue(value);
    }

    protected void attachReferenceElements(Graph graph) {
        currentMap.values().stream().filter(v -> v instanceof TraverserSet)
                .forEach(v-> attachReferenceElements((TraverserSet<Object>) v, graph));
    }

    private static void attachReferenceElements(TraverserSet<Object> toProcessTraversers, Graph graph) {
        final Iterator<Traverser.Admin<Object>> traversers = toProcessTraversers.iterator();
        while (traversers.hasNext()) {
            final Traverser.Admin<Object> traverser = traversers.next();
            Object value = traverser.get();
            if (value instanceof ReferenceVertex) {
                Vertex vertex = ((ReferenceVertex) value).attach(Attachable.Method.get(graph));
                traverser.set(vertex);
            } else if (value instanceof ReferenceEdge) {
                Edge edge = ((ReferenceEdge) value).attach(Attachable.Method.get(graph));
                traverser.set(edge);
            }
        }
    }

}

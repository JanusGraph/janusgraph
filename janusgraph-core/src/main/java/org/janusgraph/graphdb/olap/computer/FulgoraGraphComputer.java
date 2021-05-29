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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.search.path.ShortestPathVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.JanusGraphComputer;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.diskstorage.keycolumnvalue.scan.StandardScanner;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.util.WorkerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraGraphComputer implements JanusGraphComputer {

    private static final Logger log =
        LoggerFactory.getLogger(FulgoraGraphComputer.class);

    private VertexProgram<?> vertexProgram;
    private final Set<MapReduce> mapReduces = new HashSet<>();

    private final StandardJanusGraph graph;
    private final int expectedNumVertices = 10000;
    private FulgoraMemory memory;
    private FulgoraVertexMemory vertexMemory;
    private boolean executed = false;

    private int numThreads = 1;//Math.max(1,Runtime.getRuntime().availableProcessors());
    private final int readBatchSize;
    private final int writeBatchSize;

    private ResultGraph resultGraphMode = null;
    private Persist persistMode = null;

    private static final AtomicInteger computerCounter = new AtomicInteger(0);
    private final String name;
    private String jobId;

    private final GraphFilter graphFilter = new GraphFilter();

    public FulgoraGraphComputer(final StandardJanusGraph graph, final Configuration configuration) {
        this.graph = graph;
        this.writeBatchSize = configuration.get(GraphDatabaseConfiguration.BUFFER_SIZE);
        this.readBatchSize = this.writeBatchSize * 10;
        this.name = "compute" + computerCounter.incrementAndGet();
    }

    @Override
    public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        this.graphFilter.setVertexFilter(vertexFilter);
        return this;
    }

    @Override
    public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
        this.graphFilter.setEdgeFilter(edgeFilter);
        return this;
    }

    @Override
    public GraphComputer result(ResultGraph resultGraph) {
        Preconditions.checkNotNull(resultGraph, "Need to specify mode");
        this.resultGraphMode = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(Persist persist) {
        Preconditions.checkNotNull(persist, "Need to specify mode");
        this.persistMode = persist;
        return this;
    }

    @Override
    public JanusGraphComputer workers(int threads) {
        Preconditions.checkArgument(threads > 0, "Invalid number of threads: %s", threads);
        numThreads = threads;
        return this;
    }

    @Override
    public GraphComputer program(final VertexProgram vertexProgram) {
        Preconditions.checkState(this.vertexProgram == null, "A vertex program has already been set");
        this.vertexProgram = vertexProgram;
        return this;
    }

    @Override
    public GraphComputer mapReduce(final MapReduce mapReduce) {
        this.mapReduces.add(mapReduce);
        return this;
    }

    @Override
    public Future<ComputerResult> submit() {
        guardAgainstDuplicateSubmission();
        ensureSettingsAreValid();

        initializeMemory();

        return CompletableFuture.supplyAsync(this::submitAsync);
    }

    private void guardAgainstDuplicateSubmission() {
        if (executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            executed = true;
    }

    private void ensureSettingsAreValid() {
        // it is not possible execute a computer if it has no vertex program nor map-reducers
        if (null == vertexProgram && mapReduces.isEmpty())
            throw Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run map-reducers without a vertex program
        if (null != vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);
            this.mapReduces.addAll(this.vertexProgram.getMapReducers());
        }

        // if the user didn't set desired persistence/resultgraph, then get from vertex program or else, no persistence
        this.persistMode = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persistMode));
        this.resultGraphMode = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraphMode));
        // determine the legality persistence and result graph options
        if (!this.features().supportsResultGraphPersistCombination(this.resultGraphMode, this.persistMode))
            throw Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraphMode, this.persistMode);
        // ensure requested workers are not larger than supported workers
        if (this.numThreads > this.features().getMaxWorkers())
            throw Exceptions.computerRequiresMoreWorkersThanSupported(this.numThreads, this.features().getMaxWorkers());
    }

    private void initializeMemory() {
        memory = new FulgoraMemory(vertexProgram, mapReduces);
    }

    private ComputerResult submitAsync() {
        final long time = System.currentTimeMillis();
        executeVertexProgram();

        Map<MapReduce, FulgoraMapEmitter> mapJobs = collectMapJobs();
        executeMapJobs(mapJobs);

        Graph resultgraph = writeMutatedPropertiesBackIntoGraph();
        // update runtime and return the newly computed graph
        this.memory.setRuntime(System.currentTimeMillis() - time);
        this.memory.complete();
        return new DefaultComputerResult(resultgraph, this.memory);
    }

    private void executeVertexProgram() {
        if (null == vertexProgram) return;

        vertexMemory = new FulgoraVertexMemory(expectedNumVertices, graph.getIDManager(), vertexProgram);
        vertexProgram.setup(memory);

        try (VertexProgramScanJob.Executor job = VertexProgramScanJob.getVertexProgramScanJob(graph, memory, vertexMemory, vertexProgram)) {
            for (int iteration = 1; ; iteration++) {
                memory.completeSubRound();
                executeIterationOfJob(job, iteration);
                memory.completeSubRound();
                try {
                    if (this.vertexProgram.terminate(this.memory)) {
                        break;
                    }
                } finally {
                    memory.incrIteration();
                }
            }
        }
    }

    private void executeIterationOfJob(VertexProgramScanJob.Executor job, int iteration) {
        initializeVertexMemoryForIteration();
        StandardScanner.Builder scanBuilder = createScanBuilderForJob(job, iteration);
        PartitionedVertexProgramExecutor programExecutor = new PartitionedVertexProgramExecutor(graph, memory, vertexMemory, vertexProgram);
        try {
            //Iterates over all vertices and computes the vertex program on all non-partitioned vertices. For partitioned ones, the data is aggregated
            ScanMetrics jobResult = executeOnNonPartitionedVertices(iteration, scanBuilder);

            executeOnPartitionedVertices(iteration, programExecutor, jobResult);
        } catch (Exception e) {
            throw new JanusGraphException(e);
        }

        vertexMemory.completeIteration();
    }

    private void initializeVertexMemoryForIteration() {
        if (vertexProgram instanceof ShortestPathVertexProgram) {
            HashSet<MessageScope> locals = new HashSet<>();
            locals.add(MessageScope.Local.of(__::bothE));
            locals.add(MessageScope.Global.instance());
            vertexMemory.nextIteration(locals);
        } else {
            vertexMemory.nextIteration(vertexProgram.getMessageScopes(memory));
        }
    }

    private StandardScanner.Builder createScanBuilderForJob(VertexProgramScanJob.Executor job, int iteration) {
        jobId = name + "#" + iteration;
        StandardScanner.Builder scanBuilder = graph.getBackend().buildEdgeScanJob();
        scanBuilder.setJobId(jobId);
        scanBuilder.setNumProcessingThreads(numThreads);
        scanBuilder.setWorkBlockSize(readBatchSize);
        scanBuilder.setJob(job);
        return scanBuilder;
    }

    private ScanMetrics executeOnNonPartitionedVertices(int iteration, StandardScanner.Builder scanBuilder) throws InterruptedException, ExecutionException, BackendException {
        ScanMetrics jobResult = scanBuilder.execute().get();
        long failures = jobResult.get(ScanMetrics.Metric.FAILURE);
        if (failures > 0) {
            throw new JanusGraphException("Failed to process [" + failures + "] vertices in vertex program iteration " +
                "[" + iteration + "]. Computer is aborting.");
        }
        return jobResult;
    }

    private void executeOnPartitionedVertices(int iteration, PartitionedVertexProgramExecutor programExecutor, ScanMetrics jobResult) {
        programExecutor.run(numThreads, jobResult);
        long failures = jobResult.getCustom(PartitionedVertexProgramExecutor.PARTITION_VERTEX_POSTFAIL);
        if (failures > 0) {
            throw new JanusGraphException("Failed to process [" + failures + "] partitioned vertices in vertex " +
                "program iteration [" + iteration + "]. Computer is aborting.");
        }
    }

    private Map<MapReduce, FulgoraMapEmitter> collectMapJobs() {
        Map<MapReduce, FulgoraMapEmitter> mapJobs = new HashMap<>(mapReduces.size());
        for (MapReduce mapReduce : mapReduces) {
            if (mapReduce.doStage(MapReduce.Stage.MAP)) {
                FulgoraMapEmitter mapEmitter = new FulgoraMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                mapJobs.put(mapReduce, mapEmitter);
            }
        }
        return mapJobs;
    }

    private void executeMapJobs(Map<MapReduce, FulgoraMapEmitter> mapJobs) {
        jobId = name + "#map";
        try (VertexMapJob.Executor job = VertexMapJob.getVertexMapJob(graph, vertexMemory, mapJobs)) {
            executeMapJob(job);
            executeReducePhase(mapJobs);
        }
        memory.attachReferenceElements(graph);
    }

    private void executeMapJob(VertexMapJob.Executor job) {
        StandardScanner.Builder scanBuilder = graph.getBackend().buildEdgeScanJob();
        scanBuilder.setJobId(jobId);
        scanBuilder.setNumProcessingThreads(numThreads);
        scanBuilder.setWorkBlockSize(readBatchSize);
        scanBuilder.setJob(job);
        try {
            ScanMetrics jobResult = scanBuilder.execute().get();
            long failures = jobResult.get(ScanMetrics.Metric.FAILURE);
            if (failures > 0) {
                throw new JanusGraphException("Failed to process [" + failures + "] vertices in map phase. Computer is aborting.");
            }
            failures = jobResult.getCustom(VertexMapJob.MAP_JOB_FAILURE);
            if (failures > 0) {
                throw new JanusGraphException("Failed to process [" + failures + "] individual map jobs. Computer is aborting.");
            }
        } catch (JanusGraphException e) {
            throw e;
        } catch (Exception e) {
            throw new JanusGraphException(e);
        }
    }

    private void executeReducePhase(Map<MapReduce, FulgoraMapEmitter> mapJobs) {
        for (Map.Entry<MapReduce, FulgoraMapEmitter> mapJob : mapJobs.entrySet()) {
            FulgoraMapEmitter<?, ?> mapEmitter = mapJob.getValue();
            MapReduce mapReduce = mapJob.getKey();
            mapEmitter.complete(mapReduce); // sort results if a map output sort is defined
            if (mapReduce.doStage(MapReduce.Stage.REDUCE)) {
                final FulgoraReduceEmitter<?, ?> reduceEmitter = new FulgoraReduceEmitter<>();
                try (WorkerPool workers = new WorkerPool(numThreads)) {
                    workers.submit(() -> mapReduce.workerStart(MapReduce.Stage.REDUCE));
                    for (final Map.Entry queueEntry : mapEmitter.reduceMap.entrySet()) {
                        if (null == queueEntry) break;
                        workers.submit(() -> mapReduce.reduce(queueEntry.getKey(),
                            ((Iterable) queueEntry.getValue()).iterator(), reduceEmitter));
                    }
                    workers.submit(() -> mapReduce.workerEnd(MapReduce.Stage.REDUCE));
                } catch (Exception e) {
                    throw new JanusGraphException("Exception while executing reduce phase", e);
                }
//                    mapEmitter.reduceMap.entrySet().parallelStream().forEach(entry -> mapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter));

                reduceEmitter.complete(mapReduce); // sort results if a reduce output sort is defined
                mapReduce.addResultToMemory(this.memory, reduceEmitter.reduceQueue.iterator());
            } else {
                mapReduce.addResultToMemory(this.memory, mapEmitter.mapQueue.iterator());
            }
        }
    }

    private Graph writeMutatedPropertiesBackIntoGraph() {
        Graph resultgraph = graph;
        if (persistMode == Persist.NOTHING && resultGraphMode == ResultGraph.NEW) {
            resultgraph = EmptyGraph.instance();
        } else if (persistMode != Persist.NOTHING && vertexProgram != null && !vertexProgram.getVertexComputeKeys().isEmpty()) {
            //First, create property keys in graph if they don't already exist
            JanusGraphManagement management = graph.openManagement();
            try {
                for (VertexComputeKey key : vertexProgram.getVertexComputeKeys()) {
                    if (!management.containsPropertyKey(key.getKey())) {
                        log.warn("Property key [{}] is not part of the schema and will be created. It is advised to initialize all keys.", key.getKey());
                    }
                    management.getOrCreatePropertyKey(key.getKey());
                }
                management.commit();
            } finally {
                if (management != null && management.isOpen()) {
                    management.rollback();
                }
            }

            //TODO: Filter based on VertexProgram
            HaltedTraverserStrategy haltedTraverserStrategy = HaltedTraverserStrategy.reference();
            Map<Long, Map<String, Object>> mutatedProperties = Maps.transformValues(vertexMemory.getMutableVertexProperties(),
                (Function<Map<String, Object>, Map<String, Object>>) o -> {
                    Map<String, Object> nonTransientKeys =
                        Maps.filterKeys(o, s -> !VertexProgramHelper.isTransientVertexComputeKey(s, vertexProgram.getVertexComputeKeys()));
                    return Maps.transformEntries(nonTransientKeys, (k, v) -> {
                        if (k != null && k.equals(TraversalVertexProgram.HALTED_TRAVERSERS) && v instanceof TraverserSet) {
                            TraverserSet<Object> halted = new TraverserSet<>();
                            for (Traverser.Admin<Object> t : (TraverserSet<Object>) v) {
                                halted.add(haltedTraverserStrategy.halt(t));
                            }
                            return halted;
                        }
                        return v;
                    });
                });

            if (resultGraphMode == ResultGraph.ORIGINAL) {
                AtomicInteger failures = new AtomicInteger(0);
                try (WorkerPool workers = new WorkerPool(numThreads)) {
                    List<Map.Entry<Long, Map<String, Object>>> subset = new ArrayList<>(writeBatchSize / vertexProgram.getVertexComputeKeys().size());
                    int currentSize = 0;
                    for (Map.Entry<Long, Map<String, Object>> entry : mutatedProperties.entrySet()) {
                        subset.add(entry);
                        currentSize += entry.getValue().size();
                        if (currentSize >= writeBatchSize) {
                            workers.submit(new VertexPropertyWriter(subset, failures));
                            subset = new ArrayList<>(subset.size());
                            currentSize = 0;
                        }
                    }
                    if (!subset.isEmpty()) {
                        workers.submit(new VertexPropertyWriter(subset, failures));
                    }
                } catch (Exception e) {
                    throw new JanusGraphException("Exception while attempting to persist result into graph", e);
                }
                if (failures.get() > 0) {
                    throw new JanusGraphException("Could not persist program results to graph. Check log for details.");
                }
            } else if (resultGraphMode == ResultGraph.NEW) {
                resultgraph = graph.newTransaction();
                for (Map.Entry<Long, Map<String, Object>> vertexProperty : mutatedProperties.entrySet()) {
                    Vertex v = resultgraph.vertices(vertexProperty.getKey()).next();
                    for (Map.Entry<String, Object> prop : vertexProperty.getValue().entrySet()) {
                        if (prop.getValue() instanceof List) {
                            ((List) prop.getValue())
                                .forEach(value -> v.property(VertexProperty.Cardinality.list, prop.getKey(), value));
                        } else {
                            v.property(VertexProperty.Cardinality.single, prop.getKey(), prop.getValue());
                        }
                    }
                }
            }
        }
        return resultgraph;
    }

    private class VertexPropertyWriter implements Runnable {

        private final List<Map.Entry<Long, Map<String, Object>>> properties;
        private final AtomicInteger failures;

        private VertexPropertyWriter(List<Map.Entry<Long, Map<String, Object>>> properties, AtomicInteger failures) {
            assert properties != null && !properties.isEmpty() && failures != null;
            this.properties = properties;
            this.failures = failures;
        }

        @Override
        public void run() {
            JanusGraphTransaction tx = graph.buildTransaction().enableBatchLoading().start();
            try {
                for (Map.Entry<Long, Map<String, Object>> vertexProperty : properties) {
                    Vertex v = tx.getVertex(vertexProperty.getKey());
                    if (v == null) {
                        continue;
                    }
                    for (Map.Entry<String, Object> prop : vertexProperty.getValue().entrySet()) {
                        v.property(VertexProperty.Cardinality.single, prop.getKey(), prop.getValue());
                    }
                }
                tx.commit();
            } catch (Throwable e) {
                failures.incrementAndGet();
                log.error("Encountered exception while trying to write properties: ", e);
            } finally {
                if (tx != null && tx.isOpen()) tx.rollback();
            }
        }
    }

    @Override
    public String toString() {
        return StringFactory.graphComputerString(this);
    }

    @Override
    public Features features() {
        return new Features() {
            @Override
            public boolean supportsVertexAddition() {
                return false;
            }

            @Override
            public boolean supportsVertexRemoval() {
                return false;
            }

            @Override
            public boolean supportsVertexPropertyAddition() {
                return true;
            }

            @Override
            public boolean supportsVertexPropertyRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgeAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgeRemoval() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyAddition() {
                return false;
            }

            @Override
            public boolean supportsEdgePropertyRemoval() {
                return false;
            }

            @Override
            public boolean supportsGraphFilter() {
                return false;
            }

        };
    }
}

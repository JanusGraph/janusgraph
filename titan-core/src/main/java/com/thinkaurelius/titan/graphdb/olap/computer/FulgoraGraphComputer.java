package com.thinkaurelius.titan.graphdb.olap.computer;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.StandardScanner;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.util.WorkerPool;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.DefaultComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.util.GraphComputerHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FulgoraGraphComputer implements GraphComputer {

    public enum ResultMode {
        NONE, PERSIST, LOCALTX;
	
        public ResultGraph toResultGraph() {
            switch(this) {
	    case NONE: return ResultGraph.ORIGINAL;
	    case PERSIST: return ResultGraph.ORIGINAL;
	    case LOCALTX: return ResultGraph.NEW;
	    default: throw new AssertionError("Unrecognized option: " + this);
            }
        }
	
        public Persist toPersist() {
            switch(this) {
	    case NONE: return Persist.NOTHING;
	    case PERSIST: return Persist.VERTEX_PROPERTIES;
	    case LOCALTX: return Persist.VERTEX_PROPERTIES;
	    default: throw new AssertionError("Unrecognized option: " + this);
            }
        }
	
    }

    public GraphComputer resultMode(ResultMode mode) {
        result(mode.toResultGraph());
        persist(mode.toPersist());
        return this;
    }

    private static final Logger log =
            LoggerFactory.getLogger(FulgoraGraphComputer.class);

    public static final Set<VertexComputeKey> NON_PERSISTING_KEYS = ImmutableSet.of(VertexComputeKey.of(TraversalVertexProgram.HALTED_TRAVERSERS, false));

    private VertexProgram<?> vertexProgram;
    private final Set<MapReduce> mapReduces = new HashSet<>();

    private final StandardTitanGraph graph;
    private int expectedNumVertices = 10000;
    private FulgoraMemory memory;
    private FulgoraVertexMemory vertexMemory;
    private boolean executed = false;

    private int numThreads = 1;//Math.max(1,Runtime.getRuntime().availableProcessors());
    private int readBatchSize = 10000;
    private int writeBatchSize;

    private ResultGraph resultGraphMode = null;
    private Persist persistMode = null;

    private static final AtomicInteger computerCounter = new AtomicInteger(0);
    private String name;
    private String jobId;

    private final GraphFilter graphFilter = new GraphFilter();

    public FulgoraGraphComputer(final StandardTitanGraph graph, final Configuration configuration) {
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
        Preconditions.checkArgument(resultGraph != null, "Need to specify mode");
        this.resultGraphMode = resultGraph;
        return this;
    }

    @Override
    public GraphComputer persist(Persist persist) {
        Preconditions.checkArgument(persist != null, "Need to specify mode");
        this.persistMode = persist;
        return this;
    }

    @Override
    public GraphComputer workers(int threads) {
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
        if (this.executed)
            throw Exceptions.computerHasAlreadyBeenSubmittedAVertexProgram();
        else
            this.executed = true;

        // it is not possible execute a computer if it has no vertex program nor mapreducers
        if (null == vertexProgram && this.mapReduces.isEmpty())
            throw GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers();
        // it is possible to run mapreducers without a vertex program
        if (null != this.vertexProgram) {
            GraphComputerHelper.validateProgramOnComputer(this, vertexProgram);
            this.mapReduces.addAll(this.vertexProgram.getMapReducers());
        }

        // if the user didn't set desired persistence/resultgraph, then get from vertex program or else, no persistence
        this.persistMode = GraphComputerHelper.getPersistState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.persistMode));
        this.resultGraphMode = GraphComputerHelper.getResultGraphState(Optional.ofNullable(this.vertexProgram), Optional.ofNullable(this.resultGraphMode));
        // determine the legality persistence and result graph options
        if (!this.features().supportsResultGraphPersistCombination(this.resultGraphMode, this.persistMode))
            throw GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(this.resultGraphMode, this.persistMode);
	// ensure requested workers are not larger than supported workers
	if (this.numThreads > this.features().getMaxWorkers())
	    throw GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported(this.numThreads, this.features().getMaxWorkers());
	
        this.memory = new FulgoraMemory(this.vertexProgram, this.mapReduces);

        return CompletableFuture.<ComputerResult>supplyAsync(() -> {
            final long time = System.currentTimeMillis();
	    // DIVERGES: TinkerGraphComputerView and try (final TinkerWorkerPool workers = new TinkerWorkerPool(this.workers)) {
            if (null != vertexProgram) {
                // ##### Execute vertex program - DIVERGES: no FulgoraVertexMemory in TinkerGraphComputer, and no view in Fulgora
                vertexMemory = new FulgoraVertexMemory(expectedNumVertices, graph.getIDManager(), vertexProgram);
                // execute the vertex program
                this.vertexProgram.setup(this.memory);
		// DIVERGES: This is inside the while (true) { loop in TinkerGraphComputer... will put inExecute... hmmm...
                // memory.completeSubRound();

                for (int iteration = 1; ; iteration++) {
		    // Added here instead?
		    memory.completeSubRound();
                    vertexMemory.nextIteration(vertexProgram.getMessageScopes(memory));

                    jobId = name + "#" + iteration;
                    VertexProgramScanJob.Executor job = VertexProgramScanJob.getVertexProgramScanJob(graph, memory, vertexMemory, vertexProgram);
                    StandardScanner.Builder scanBuilder = graph.getBackend().buildEdgeScanJob();
                    scanBuilder.setJobId(jobId);
                    scanBuilder.setNumProcessingThreads(numThreads);
                    scanBuilder.setWorkBlockSize(readBatchSize);
                    scanBuilder.setJob(job);
                    PartitionedVertexProgramExecutor pvpe = new PartitionedVertexProgramExecutor(graph, memory, vertexMemory, vertexProgram);
                    try {
                        //Iterates over all vertices and computes the vertex program on all non-partitioned vertices. For partitioned ones, the data is aggregated
                        ScanMetrics jobResult = scanBuilder.execute().get();
                        long failures = jobResult.get(ScanMetrics.Metric.FAILURE);
                        if (failures > 0) {
                            throw new TitanException("Failed to process [" + failures + "] vertices in vertex program iteration [" + iteration + "]. Computer is aborting.");
                        }
                        //Runs the vertex program on all aggregated, partitioned vertices.
                        pvpe.run(numThreads, jobResult);
                        failures = jobResult.getCustom(PartitionedVertexProgramExecutor.PARTITION_VERTEX_POSTFAIL);
                        if (failures > 0) {
                            throw new TitanException("Failed to process [" + failures + "] partitioned vertices in vertex program iteration [" + iteration + "]. Computer is aborting.");
                        }
                    } catch (Exception e) {
                        throw new TitanException(e);
                    }

                    this.vertexMemory.completeIteration();
                    this.memory.completeSubRound();
                    try {
                        if (this.vertexProgram.terminate(this.memory)) {
                            break;
                        }
                    } finally {
                        this.memory.incrIteration();
                    }
                }
            }
	    // DIVERGE: Missing the MapReduce only stuff here... still need to figure out the difference between the view and Titan's setup

            // ##### Execute mapreduce jobs
            // Collect map jobs
            Map<MapReduce, FulgoraMapEmitter> mapJobs = new HashMap<>(mapReduces.size());
            for (MapReduce mapReduce : mapReduces) {
                if (mapReduce.doStage(MapReduce.Stage.MAP)) {
                    FulgoraMapEmitter mapEmitter = new FulgoraMapEmitter<>(mapReduce.doStage(MapReduce.Stage.REDUCE));
                    mapJobs.put(mapReduce, mapEmitter);
                }
            }
            // Execute map jobs
            jobId = name + "#map";
            VertexMapJob.Executor job = VertexMapJob.getVertexMapJob(graph, vertexMemory, mapJobs);
            StandardScanner.Builder scanBuilder = graph.getBackend().buildEdgeScanJob();
            scanBuilder.setJobId(jobId);
            scanBuilder.setNumProcessingThreads(numThreads);
            scanBuilder.setWorkBlockSize(readBatchSize);
            scanBuilder.setJob(job);
            try {
                ScanMetrics jobResult = scanBuilder.execute().get();
                long failures = jobResult.get(ScanMetrics.Metric.FAILURE);
                if (failures > 0) {
                    throw new TitanException("Failed to process [" + failures + "] vertices in map phase. Computer is aborting.");
                }
                failures = jobResult.getCustom(VertexMapJob.MAP_JOB_FAILURE);
                if (failures > 0) {
                    throw new TitanException("Failed to process [" + failures + "] individual map jobs. Computer is aborting.");
                }
            } catch (Exception e) {
                throw new TitanException(e);
            }
            // Execute reduce phase and add to memory
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
                            workers.submit(() -> mapReduce.reduce(queueEntry.getKey(), ((Iterable) queueEntry.getValue()).iterator(), reduceEmitter));
                        }
                        workers.submit(() -> mapReduce.workerEnd(MapReduce.Stage.REDUCE));
                    } catch (Exception e) {
                        throw new TitanException("Exception while executing reduce phase", e);
                    }
//                    mapEmitter.reduceMap.entrySet().parallelStream().forEach(entry -> mapReduce.reduce(entry.getKey(), entry.getValue().iterator(), reduceEmitter));


                    reduceEmitter.complete(mapReduce); // sort results if a reduce output sort is defined
                    mapReduce.addResultToMemory(this.memory, reduceEmitter.reduceQueue.iterator());
                } else {
                    mapReduce.addResultToMemory(this.memory, mapEmitter.mapQueue.iterator());
                }
            }

            // #### Write mutated properties back into graph
            Graph resultgraph = graph;
            if (persistMode == Persist.NOTHING && resultGraphMode == ResultGraph.NEW) {
                resultgraph = EmptyGraph.instance();
            } else if (persistMode != Persist.NOTHING && vertexProgram != null && !vertexProgram.getVertexComputeKeys().isEmpty()) {
                //First, create property keys in graph if they don't already exist
                TitanManagement mgmt = graph.openManagement();
                try {
                    for (VertexComputeKey key : vertexProgram.getVertexComputeKeys()) {
                        if (!mgmt.containsPropertyKey(key.getKey()))
                            log.warn("Property key [{}] is not part of the schema and will be created. It is advised to initialize all keys.", key.getKey());
                        mgmt.getOrCreatePropertyKey(key.getKey());
                    }
                    mgmt.commit();
                } finally {
                    if (mgmt != null && mgmt.isOpen()) mgmt.rollback();
                }

                //TODO: Filter based on VertexProgram
                Map<Long, Map<String, Object>> mutatedProperties = Maps.transformValues(vertexMemory.getMutableVertexProperties(),
                        new Function<Map<String, Object>, Map<String, Object>>() {
                            @Nullable
                            @Override
                            public Map<String, Object> apply(@Nullable Map<String, Object> o) {
                                return Maps.filterKeys(o, s -> !NON_PERSISTING_KEYS.contains(s));
                            }
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
                        if (!subset.isEmpty()) workers.submit(new VertexPropertyWriter(subset, failures));
                    } catch (Exception e) {
                        throw new TitanException("Exception while attempting to persist result into graph", e);
                    }
                    if (failures.get() > 0)
                        throw new TitanException("Could not persist program results to graph. Check log for details.");
                } else if (resultGraphMode == ResultGraph.NEW) {
                    resultgraph = graph.newTransaction();
                    for (Map.Entry<Long, Map<String, Object>> vprop : mutatedProperties.entrySet()) {
                        Vertex v = resultgraph.vertices(vprop.getKey()).next();
                        for (Map.Entry<String, Object> prop : vprop.getValue().entrySet()) {
                            v.property(VertexProperty.Cardinality.single, prop.getKey(), prop.getValue());
                        }
                    }
                }
            }
            // update runtime and return the newly computed graph
            this.memory.setRuntime(System.currentTimeMillis() - time);
            this.memory.complete();
            return new DefaultComputerResult(resultgraph, this.memory);
        });
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
            TitanTransaction tx = graph.buildTransaction().enableBatchLoading().start();
            try {
                for (Map.Entry<Long, Map<String, Object>> vprop : properties) {
                    Vertex v = tx.getVertex(vprop.getKey());
                    for (Map.Entry<String, Object> prop : vprop.getValue().entrySet()) {
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
            public boolean supportsResultGraphPersistCombination(final ResultGraph resultGraph, final Persist persist) {
                return persist == Persist.NOTHING || persist == Persist.VERTEX_PROPERTIES;
            }

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

        };
    }
}

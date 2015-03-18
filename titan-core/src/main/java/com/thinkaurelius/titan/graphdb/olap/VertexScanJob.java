package com.thinkaurelius.titan.graphdb.olap;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;

/**
 * Expresses a computation over all vertices ina a graph database. Process is called for each vertex that is previously
 * loaded from disk. To limit the data that needs to be pulled out of the database, the query can specify the queries
 * (which need to be vertex-centric) for the data that is needed. Only this data is then pulled. If the user attempts
 * to access additional data during processing, the behavior is undefined.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface VertexScanJob extends Cloneable {

    /**
     * @see com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob
     *
     * @param graph
     * @param config
     * @param metrics
     */
    public default void workerIterationStart(TitanGraph graph, Configuration config, ScanMetrics metrics) {}

    /**
     * @see com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob
     *
     * @param metrics
     */
    public default void workerIterationEnd(ScanMetrics metrics) {}

    /**
     * Process the given vertex with its adjacency list and properties pre-loaded.
     *
     * @param vertex
     * @param metrics
     */
    public void process(TitanVertex vertex, ScanMetrics metrics);


    /**
     * Specify the queries for the data to be loaded into the vertices prior to processing.
     *
     * @param queries
     */
    public void getQueries(QueryContainer queries);

    /**
     * Returns a clone of this VertexScanJob. The clone will not yet be initialized for computation but all of
     * its internal state (if any) must match that of the original copy.
     *
     * @return A clone of this job
     */
    public VertexScanJob clone();

}

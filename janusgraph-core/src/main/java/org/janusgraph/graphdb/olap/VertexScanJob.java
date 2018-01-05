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

package org.janusgraph.graphdb.olap;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;

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
     * @see org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob
     *
     * @param graph
     * @param config
     * @param metrics
     */
    default void workerIterationStart(JanusGraph graph, Configuration config, ScanMetrics metrics) {}

    /**
     * @see org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob
     *
     * @param metrics
     */
    default void workerIterationEnd(ScanMetrics metrics) {}

    /**
     * Process the given vertex with its adjacency list and properties pre-loaded.
     *
     * @param vertex
     * @param metrics
     */
    void process(JanusGraphVertex vertex, ScanMetrics metrics);


    /**
     * Specify the queries for the data to be loaded into the vertices prior to processing.
     *
     * @param queries
     */
    void getQueries(QueryContainer queries);

    /**
     * Returns a clone of this VertexScanJob. The clone will not yet be initialized for computation but all of
     * its internal state (if any) must match that of the original copy.
     *
     * @return A clone of this job
     */
    VertexScanJob clone();

}

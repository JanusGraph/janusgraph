package com.thinkaurelius.titan.graphdb.database.management;


import com.thinkaurelius.titan.core.schema.SchemaStatus;

import java.time.Duration;
import java.util.Map;

public class GraphIndexStatusReport {
    private final boolean success;
    private final String indexName;
    private final SchemaStatus targetStatus;
    private final Map<String, SchemaStatus> notConverged;
    private final Map<String, SchemaStatus> converged;
    private final Duration elapsed;

    public GraphIndexStatusReport(boolean success, String indexName, SchemaStatus targetStatus,
                   Map<String, SchemaStatus> notConverged,
                   Map<String, SchemaStatus> converged, Duration elapsed) {
        this.success = success;
        this.indexName = indexName;
        this.targetStatus = targetStatus;
        this.notConverged = notConverged;
        this.converged = converged;
        this.elapsed = elapsed;
    }

    public boolean getSucceeded() {
        return success;
    }

    public String getIndexName() {
        return indexName;
    }

    public SchemaStatus getTargetStatus() {
        return targetStatus;
    }

    public Map<String, SchemaStatus> getNotConvergedKeys() {
        return notConverged;
    }

    public Map<String, SchemaStatus> getConvergedKeys() {
        return converged;
    }

    public Duration getElapsed() {
        return elapsed;
    }

    @Override
    public String toString() {
        return "GraphIndexStatusReport[" +
                "success=" + success +
                ", indexName='" + indexName + '\'' +
                ", targetStatus=" + targetStatus +
                ", notConverged=" + notConverged +
                ", converged=" + converged +
                ", elapsed=" + elapsed +
                ']';
    }
}
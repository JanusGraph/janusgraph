package com.thinkaurelius.titan.graphdb.database.management;


import com.thinkaurelius.titan.core.schema.SchemaStatus;

import java.time.Duration;

public class RelationIndexStatusReport {

    private final boolean succeeded;
    private final String indexName;
    private final String relationTypeName;
    private final SchemaStatus actualStatus;
    private final SchemaStatus targetStatus;
    private final Duration elapsed;

    public RelationIndexStatusReport(boolean succeeded, String indexName, String relationTypeName, SchemaStatus actualStatus,
                                   SchemaStatus targetStatus, Duration elapsed) {
        this.succeeded = succeeded;
        this.indexName = indexName;
        this.relationTypeName = relationTypeName;
        this.actualStatus = actualStatus;
        this.targetStatus = targetStatus;
        this.elapsed = elapsed;
    }

    public boolean getSucceeded() {
        return succeeded;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getRelationTypeName() {
        return relationTypeName;
    }

    public SchemaStatus getActualStatus() {
        return actualStatus;
    }

    public SchemaStatus getTargetStatus() {
        return targetStatus;
    }

    public Duration getElapsed() {
        return elapsed;
    }

    @Override
    public String toString() {
        return "RelationIndexStatusReport[" +
                "succeeded=" + succeeded +
                ", indexName='" + indexName + '\'' +
                ", relationTypeName='" + relationTypeName + '\'' +
                ", actualStatus=" + actualStatus +
                ", targetStatus=" + targetStatus +
                ", elapsed=" + elapsed +
                ']';
    }
}
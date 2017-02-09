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

package org.janusgraph.graphdb.database.management;


import org.janusgraph.core.schema.SchemaStatus;

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

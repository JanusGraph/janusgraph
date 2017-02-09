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

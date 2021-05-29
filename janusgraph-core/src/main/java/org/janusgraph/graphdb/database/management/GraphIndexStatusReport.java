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
import java.util.List;
import java.util.Map;

public class GraphIndexStatusReport extends AbstractIndexStatusReport {
    private final Map<String, SchemaStatus> notConverged;
    private final Map<String, SchemaStatus> converged;

    public GraphIndexStatusReport(boolean success, String indexName, List<SchemaStatus> targetStatuses,
                   Map<String, SchemaStatus> notConverged,
                   Map<String, SchemaStatus> converged, Duration elapsed) {
        super(success, indexName, targetStatuses, elapsed);
        this.notConverged = notConverged;
        this.converged = converged;
    }

    public Map<String, SchemaStatus> getNotConvergedKeys() {
        return notConverged;
    }

    public Map<String, SchemaStatus> getConvergedKeys() {
        return converged;
    }

    @Override
    public String toString() {
        return "GraphIndexStatusReport[" +
                "success=" + success +
                ", indexName='" + indexName + '\'' +
                ", targetStatus=" + targetStatuses +
                ", notConverged=" + notConverged +
                ", converged=" + converged +
                ", elapsed=" + elapsed +
                ']';
    }
}


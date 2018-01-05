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

public abstract class AbstractIndexStatusReport {
    protected final boolean success;
    protected final String indexName;
    protected final List<SchemaStatus> targetStatuses;
    protected final Duration elapsed;

    public AbstractIndexStatusReport(boolean success, String indexName, List<SchemaStatus> targetStatuses, Duration elapsed) {
        this.success = success;
        this.indexName = indexName;
        this.targetStatuses = targetStatuses;
        this.elapsed = elapsed;
    }

    public boolean getSucceeded() {
        return success;
    }

    public String getIndexName() {
        return indexName;
    }

    public List<SchemaStatus> getTargetStatuses() {
        return targetStatuses;
    }

    public Duration getElapsed() {
        return elapsed;
    }
}


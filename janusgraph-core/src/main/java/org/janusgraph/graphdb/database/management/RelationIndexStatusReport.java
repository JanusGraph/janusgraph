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

public class RelationIndexStatusReport extends AbstractIndexStatusReport {

    private final String relationTypeName;
    private final SchemaStatus actualStatus;

    public RelationIndexStatusReport(boolean success, String indexName, String relationTypeName, SchemaStatus actualStatus,
                                   List<SchemaStatus>targetStatuses, Duration elapsed) {
        super(success, indexName, targetStatuses, elapsed);
        this.relationTypeName = relationTypeName;
        this.actualStatus = actualStatus;
    }

    public String getRelationTypeName() {
        return relationTypeName;
    }

    public SchemaStatus getActualStatus() {
        return actualStatus;
    }

    @Override
    public String toString() {
        return "RelationIndexStatusReport[" +
                "succeeded=" + success +
                ", indexName='" + indexName + '\'' +
                ", relationTypeName='" + relationTypeName + '\'' +
                ", actualStatus=" + actualStatus +
                ", targetStatus=" + targetStatuses +
                ", elapsed=" + elapsed +
                ']';
    }
}

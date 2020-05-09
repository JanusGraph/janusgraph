// Copyright 2020 JanusGraph Authors
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

import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.cache.SchemaCache;
import org.janusgraph.graphdb.database.serialize.StandardSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.EnumSerializer;

public class CfgManagementLoggerFactory extends ManagementLoggerFactory {
    @Override
    public ManagementLogger createManagementLogger(StandardJanusGraph graph, Log log, SchemaCache schemaCache, TimestampProvider times) {
        StandardSerializer serializer = (StandardSerializer) graph.getDataSerializer();
        if (!serializer.checkIfRegistrationExists(69)) {
            serializer.registerClassInternal(69, GraphCacheEvictionAction.class, new EnumSerializer<>(GraphCacheEvictionAction.class));
        }
        return new CfgManagementLogger(graph, log, schemaCache, times);
    }

}

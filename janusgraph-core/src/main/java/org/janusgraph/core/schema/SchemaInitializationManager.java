// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.core.schema;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.json.creator.SchemaCreationException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class SchemaInitializationManager {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaInitializationManager.class);

    public static final Map<String, SchemaInitStrategy> SCHEMA_INIT_STRATEGIES;

    static {
        SCHEMA_INIT_STRATEGIES = new HashMap<>(1);
        SCHEMA_INIT_STRATEGIES.put(SchemaInitType.NONE.getConfigName(), new NoneSchemaInitStrategy());
        SCHEMA_INIT_STRATEGIES.put(SchemaInitType.JSON.getConfigName(), new JsonSchemaInitStrategy());
    }

    public static JanusGraph initializeSchemaAndStart(GraphDatabaseConfiguration graphDatabaseConfiguration){

        // drop schema before init is selected
        if(graphDatabaseConfiguration.isDropSchemaBeforeInit()){
            try {
                LOG.info("Dropping current JanusGraph schema and data.");
                JanusGraphFactory.drop(new StandardJanusGraph(graphDatabaseConfiguration));
                LOG.info("Current JanusGraph schema and data were removed.");
            } catch (BackendException e) {
                throw new SchemaCreationException(e);
            }
        }

        // get schema initialization strategy
        String schemaInitStrategyPath = graphDatabaseConfiguration.getSchemaInitStrategy();
        SchemaInitStrategy schemaInitStrategy = SCHEMA_INIT_STRATEGIES.get(schemaInitStrategyPath);
        if(schemaInitStrategy == null){
            try {
                Class schemaInitStrategyClass = Class.forName(schemaInitStrategyPath);
                schemaInitStrategy = (SchemaInitStrategy) schemaInitStrategyClass.getDeclaredConstructor().newInstance();
            } catch (ClassNotFoundException | InvocationTargetException | InstantiationException |
                     IllegalAccessException | NoSuchMethodException e) {
                throw new SchemaCreationException(e);
            }
        }

        // initialize schema
        JanusGraph graph = schemaInitStrategy.initializeSchemaAndStart(graphDatabaseConfiguration);

        if(graph == null){
            graph = new StandardJanusGraph(graphDatabaseConfiguration);
        }

        return graph;
    }

}

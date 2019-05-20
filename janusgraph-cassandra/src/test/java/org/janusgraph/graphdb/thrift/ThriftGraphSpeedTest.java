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

package org.janusgraph.graphdb.thrift;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.graphdb.SpeedTestSchema;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.graphdb.JanusGraphSpeedTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.testcategory.PerformanceTests;

@Category({PerformanceTests.class})
public class ThriftGraphSpeedTest extends JanusGraphSpeedTest {

    private static StandardJanusGraph graph;
    private static SpeedTestSchema schema;

    private static final Logger log =
            LoggerFactory.getLogger(ThriftGraphSpeedTest.class);

    public ThriftGraphSpeedTest() throws BackendException {
        super(CassandraStorageSetup.getCassandraThriftGraphConfiguration(ThriftGraphSpeedTest.class.getSimpleName()));
    }

    @BeforeClass
    public static void beforeClass() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    protected StandardJanusGraph getGraph() throws BackendException {


        if (null == graph) {
            GraphDatabaseConfiguration graphDatabaseConfiguration = new GraphDatabaseConfiguration(conf);
            graphDatabaseConfiguration.getBackend().clearStorage();
            log.debug("Cleared backend storage");
            graph = (StandardJanusGraph)JanusGraphFactory.open(conf);
            initializeGraph(graph);
        }
        return graph;
    }

    @Override
    protected SpeedTestSchema getSchema() {
        if (null == schema) {
            schema = SpeedTestSchema.get();
        }
        return schema;
    }
}

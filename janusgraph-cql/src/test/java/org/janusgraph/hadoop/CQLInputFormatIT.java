// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.hadoop;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.janusgraph.JanusGraphCassandraContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Testcontainers
public class CQLInputFormatIT extends AbstractInputFormatIT {

    @Container
    private static JanusGraphCassandraContainer cql = new JanusGraphCassandraContainer();

    private PropertiesConfiguration getGraphConfiguration(final String filename) throws ConfigurationException, IOException {
        final PropertiesConfiguration config = ConfigurationUtil.loadPropertiesConfig(filename, false);
        Path baseOutDir = Paths.get((String) config.getProperty("gremlin.hadoop.outputLocation"));
        baseOutDir.toFile().mkdirs();
        String outDir = Files.createTempDirectory(baseOutDir, null).toAbsolutePath().toString();
        config.setProperty("gremlin.hadoop.outputLocation", outDir);
        config.setProperty("janusgraphmr.ioformat.conf.storage.port", String.valueOf(cql.getMappedCQLPort()));
        return config;
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return cql.getConfiguration("cqlinputformatit").getConfiguration();
    }

    @Override
    protected Graph getGraph() throws ConfigurationException, IOException {
        return GraphFactory.open(getGraphConfiguration("target/test-classes/cql-read.properties"));
    }

    @Test
    public void testOpenFromConfigWithMultiHosts() throws ConfigurationException, IOException {
        final Graph g = GraphFactory.open(getGraphConfiguration("target/test-classes/cql-read-multi-hosts.properties"));
        runTraversalWithInvalidHost(g, "invalid-host");
    }

    @Test
    public void testOpenFromFileWithMultiHosts() throws ConfigurationException, IOException {
        final Graph g = GraphFactory.open("target/test-classes/cql-read-multi-hosts.properties");
        runTraversalWithInvalidHost(g, "invalid-host");
    }

    private void runTraversalWithInvalidHost(final Graph g, final String hostname) {
        final GraphTraversalSource t = g.traversal().withComputer(SparkGraphComputer.class);
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> t.V().next());
        assertEquals("java.lang.IllegalArgumentException: Failed to add contact point: " + hostname, exception.getMessage());
    }
}

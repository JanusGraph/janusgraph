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

package org.janusgraph.hadoop;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.janusgraph.JanusGraphCassandraThriftContainer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Testcontainers
public class CassandraInputFormatIT extends AbstractInputFormatIT {

    @Container
    public static JanusGraphCassandraThriftContainer thriftContainer = new JanusGraphCassandraThriftContainer(true);

    protected PropertiesConfiguration getGraphConfiguration() throws ConfigurationException, IOException {
        final PropertiesConfiguration config = new PropertiesConfiguration("target/test-classes/cassandra-read.properties");
        Path baseOutDir = Paths.get((String) config.getProperty("gremlin.hadoop.outputLocation"));
        baseOutDir.toFile().mkdirs();
        String outDir = Files.createTempDirectory(baseOutDir, null).toAbsolutePath().toString();
        config.setProperty("gremlin.hadoop.outputLocation", outDir);
        config.setProperty("janusgraphmr.ioformat.conf.storage.port", String.valueOf(thriftContainer.getMappedThirftPort()));
        return config;
    }

    protected Graph getGraph() throws ConfigurationException, IOException {
        return GraphFactory.open(getGraphConfiguration());
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return thriftContainer.getThriftConfiguration(getClass().getSimpleName()).getConfiguration();
    }
}

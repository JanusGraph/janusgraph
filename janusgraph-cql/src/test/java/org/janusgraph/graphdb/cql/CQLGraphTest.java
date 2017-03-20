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

package org.janusgraph.graphdb.cql;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.KEYSPACE;
import static org.junit.Assert.*;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.cql.CassandraStorageSetup;
import org.janusgraph.graphdb.CassandraGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.JanusGraphConstants;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.Test;

public class CQLGraphTest extends CassandraGraphTest {

    @Override
    public WriteConfiguration getConfiguration() {
        return CassandraStorageSetup.getCQLConfiguration(getClass().getSimpleName()).getConfiguration();
    }

    @Test
    public void testTitanGraphBackwardCompatibility() {
        close();
        WriteConfiguration wc = getConfiguration();
        wc.set(ConfigElement.getPath(KEYSPACE), "titan");
        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "x.x.x");

        assertNull(wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION),
                GraphDatabaseConfiguration.INITIAL_JANUSGRAPH_VERSION.getDatatype()));

        assertFalse(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
                wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS),
                        GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())));

        wc.set(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS), "1.0.0");
        assertTrue(JanusGraphConstants.TITAN_COMPATIBLE_VERSIONS.contains(
                wc.get(ConfigElement.getPath(GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS),
                        GraphDatabaseConfiguration.TITAN_COMPATIBLE_VERSIONS.getDatatype())));

        graph = (StandardJanusGraph) JanusGraphFactory.open(wc);
    }
}

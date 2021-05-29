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

package org.janusgraph.graphdb.inmemory;

import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class InMemoryConfigurationTest {

    StandardJanusGraph graph;

    public void initialize(ConfigOption option, Object value) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        config.set(option,value);
        graph = (StandardJanusGraph) JanusGraphFactory.open(config);
    }

    @AfterEach
    public void shutdown() {
        graph.close();
    }


    @Test
    public void testReadOnly() {
        initialize(GraphDatabaseConfiguration.STORAGE_READONLY,true);

        JanusGraphTransaction tx = graph.newTransaction();
        try {
            tx.addVertex();
            fail();
        } catch (Exception ignored) {
        } finally {
            tx.rollback();
        }

        try {
            graph.addVertex();
            fail();
        } catch (Exception ignored) {
        } finally {
            graph.tx().rollback();
        }

    }


}

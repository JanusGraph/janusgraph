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

package org.janusgraph.diskstorage.es;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.system.IOUtils;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.junit.jupiter.Container;

import java.io.File;

import static org.janusgraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

@Testcontainers
public class BerkeleyElasticsearchTest extends JanusGraphIndexTest {

    @Container
    private static JanusGraphElasticsearchContainer esr = new JanusGraphElasticsearchContainer();

    public BerkeleyElasticsearchTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        return esr.setConfiguration(getBerkeleyJEConfiguration(), INDEX)
            .set(GraphDatabaseConfiguration.INDEX_MAX_RESULT_SET_SIZE, 3, INDEX)
            .getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }
    @Override
    public boolean supportsWildcardQuery() {
        return true;
    }
    @Override
    protected boolean supportsCollections() {
        return true;
    }

    /**
     * Test {@link org.janusgraph.example.GraphOfTheGodsFactory#create(String)}.
     */
    @Test
    public void testGraphOfTheGodsFactoryCreate() {
        File bdbtmp = new File("target/gotgfactory");
        IOUtils.deleteDirectory(bdbtmp, true);

        JanusGraph gotg = JanusGraphFactory.open(getConfiguration());
        GraphOfTheGodsFactory.load(gotg);
        JanusGraphIndexTest.assertGraphOfTheGods(gotg);
        gotg.close();
    }
}

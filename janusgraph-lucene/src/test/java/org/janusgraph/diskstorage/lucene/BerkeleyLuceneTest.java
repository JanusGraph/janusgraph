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

package org.janusgraph.diskstorage.lucene;

import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.Test;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
import static org.janusgraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class BerkeleyLuceneTest extends JanusGraphIndexTest {

    public BerkeleyLuceneTest() {
        super(true, true, true);
    }

    @Override
    public WriteConfiguration getConfiguration() {
        ModifiableConfiguration config = getBerkeleyJEConfiguration();
        for (String indexBackend : getIndexBackends()) {
            config.set(INDEX_BACKEND, "lucene", indexBackend);
            config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"), indexBackend);
        }
        return config.getConfiguration();
    }

    @Override
    public boolean supportsLuceneStyleQueries() {
        return true;
    }

    @Override
    public boolean supportsGeoPointExistsQuery() {
        return false;
    }

    @Override
    public boolean supportsWildcardQuery() {
        return false;
    }

    @Override
    protected boolean supportsCollections() {
        return true;
    }

    @Override
    protected boolean supportsGeoCollections() {
        return false;
    }

    @Test
    public void testPrintSchemaElements() {
        GraphOfTheGodsFactory.load(graph);
        mgmt = graph.openManagement();

        String expected = "A18422278042949EE2B162837EC27A63";
        String output = mgmt.printSchema();
        String outputHash = DigestUtils.md5Hex(output).toUpperCase();
        assertEquals(expected, outputHash);

        expected = "2114C009DC359B1C9AD7D0655AC6C9BF";
        output = mgmt.printVertexLabels();
        outputHash = DigestUtils.md5Hex(output).toUpperCase();
        assertEquals(expected, outputHash);

        expected = "1E8AAE2C887544E490948F2ACBBFE312";
        output = mgmt.printEdgeLabels();
        outputHash = DigestUtils.md5Hex(output).toUpperCase();
        assertEquals(expected, outputHash);

        expected = "35851C8867321C8CB3E275886F40E8B9";
        output = mgmt.printPropertyKeys();
        outputHash = DigestUtils.md5Hex(output).toUpperCase();
        assertEquals(expected, outputHash);

        expected = "3951BBB935D39EE0FF74CE2D5F7CA997";
        output = mgmt.printIndexes();
        outputHash = DigestUtils.md5Hex(output).toUpperCase();
        assertEquals(expected, outputHash);
    }

}

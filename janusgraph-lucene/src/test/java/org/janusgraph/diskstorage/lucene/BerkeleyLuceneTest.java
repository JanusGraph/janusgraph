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

import org.apache.commons.codec.digest.DigestUtils;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphIndexTest;
import org.junit.jupiter.api.Test;

import static org.janusgraph.BerkeleyStorageSetup.getBerkeleyJEConfiguration;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;
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

        String expected = "026DCB18B35C198CC54FA66B3D86C3FB";
        String output = mgmt.printSchema();
        String outputHash = DigestUtils.md5Hex(output).toUpperCase();
        assertEquals(expected, outputHash);

        String expectedVertexLabels =
            "------------------------------------------------------------------------------------------------\n" +
             "Vertex Label Name              | Partitioned | Static                                             |\n" +
             "---------------------------------------------------------------------------------------------------\n" +
             "titan                          | false       | false                                              |\n" +
             "location                       | false       | false                                              |\n" +
             "god                            | false       | false                                              |\n" +
             "demigod                        | false       | false                                              |\n" +
             "human                          | false       | false                                              |\n" +
             "monster                        | false       | false                                              |\n" +
             "---------------------------------------------------------------------------------------------------\n";
        assertEquals(expectedVertexLabels, mgmt.printVertexLabels());

        String expectedEdgeLabels =
            "------------------------------------------------------------------------------------------------\n" +
            "Edge Label Name                | Directed    | Unidirected | Multiplicity                         |\n" +
            "---------------------------------------------------------------------------------------------------\n" +
            "brother                        | true        | false       | MULTI                                |\n" +
            "father                         | true        | false       | MANY2ONE                             |\n" +
            "mother                         | true        | false       | MANY2ONE                             |\n" +
            "battled                        | true        | false       | MULTI                                |\n" +
            "lives                          | true        | false       | MULTI                                |\n" +
            "pet                            | true        | false       | MULTI                                |\n" +
            "---------------------------------------------------------------------------------------------------\n";
        assertEquals(expectedEdgeLabels, mgmt.printEdgeLabels());

        String expectedPropertyKeys =
            "------------------------------------------------------------------------------------------------\n" +
            "Property Key Name              | Cardinality | Data Type                                          |\n" +
            "---------------------------------------------------------------------------------------------------\n" +
            "name                           | SINGLE      | class java.lang.String                             |\n" +
            "age                            | SINGLE      | class java.lang.Integer                            |\n" +
            "time                           | SINGLE      | class java.lang.Integer                            |\n" +
            "reason                         | SINGLE      | class java.lang.String                             |\n" +
            "place                          | SINGLE      | class org.janusgraph.core.attribute.Geoshape       |\n" +
            "---------------------------------------------------------------------------------------------------\n";
        assertEquals(expectedPropertyKeys, mgmt.printPropertyKeys());

        String expectedIndexes =
            "------------------------------------------------------------------------------------------------\n" +
            "Graph Index (Vertex)           | Type        | Unique    | Backing        | Key:           Status |\n" +
            "---------------------------------------------------------------------------------------------------\n" +
            "name                           | Composite   | true      | internalindex  | name:         ENABLED |\n" +
            "vertices                       | Mixed       | false     | search         | age:          ENABLED |\n" +
            "---------------------------------------------------------------------------------------------------\n" +
            "Graph Index (Edge)             | Type        | Unique    | Backing        | Key:           Status |\n" +
            "---------------------------------------------------------------------------------------------------\n" +
            "edges                          | Mixed       | false     | search         | reason:       ENABLED |\n" +
            "                               |             |           |                | place:        ENABLED |\n" +
            "---------------------------------------------------------------------------------------------------\n" +
            "Relation Index (VCI)           | Type        | Direction | Sort Key       | Order    |     Status |\n" +
            "---------------------------------------------------------------------------------------------------\n" +
            "battlesByTime                  | battled     | BOTH      | time           | desc     |    ENABLED |\n" +
            "---------------------------------------------------------------------------------------------------\n";
        assertEquals(expectedIndexes, mgmt.printIndexes());
    }

}

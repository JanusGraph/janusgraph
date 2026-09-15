// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.tablestore;

import org.janusgraph.TableStoreContainer;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.JanusGraphCustomIdIndexTest;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;


@Testcontainers
public class TableStoreLuceneCustomIdTest extends JanusGraphCustomIdIndexTest {

    @Container
    public static final TableStoreContainer tableStoreContainer = new TableStoreContainer();

    @Override
    protected ModifiableConfiguration getModifiableConfiguration() {
        ModifiableConfiguration config = tableStoreContainer.getModifiableConfiguration();
        for (String indexBackend : getIndexBackends()) {
            config.set(INDEX_BACKEND, "lucene", indexBackend);
            config.set(INDEX_DIRECTORY, StorageSetup.getHomeDir("lucene"), indexBackend);
        }
        return config;
    }
}

// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import static org.janusgraph.StorageSetup.getHomeDir;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_DIRECTORY;

public class BerkeleyLuceneIndexMetricTest extends IndexMetricTest {

    @Override
    public WriteConfiguration getConfiguration() {
        WriteConfiguration config = BerkeleyStorageSetup.getBerkeleyJEGraphConfiguration();
        ModifiableConfiguration modifiableConfiguration =
            new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, config, BasicConfiguration.Restriction.NONE);
        modifiableConfiguration.set(BASIC_METRICS, true);
        modifiableConfiguration.set(INDEX_BACKEND, "lucene", "search");
        modifiableConfiguration.set(INDEX_DIRECTORY, getHomeDir("lucene"), "search");
        return config;
    }
}

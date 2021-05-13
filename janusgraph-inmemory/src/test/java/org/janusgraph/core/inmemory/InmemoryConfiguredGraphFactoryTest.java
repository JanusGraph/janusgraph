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

package org.janusgraph.core.inmemory;

import org.apache.commons.configuration2.MapConfiguration;
import org.janusgraph.core.AbstractConfiguredGraphFactoryTest;
import org.janusgraph.util.system.ConfigurationUtil;

import java.util.HashMap;
import java.util.Map;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.GRAPH_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

public class InmemoryConfiguredGraphFactoryTest extends AbstractConfiguredGraphFactoryTest {

    protected MapConfiguration getManagementConfig() {
        final Map<String, Object> map = new HashMap<>();
        map.put(STORAGE_BACKEND.toStringWithoutRoot(), "inmemory");
        return ConfigurationUtil.loadMapConfiguration(map);
    }

    protected MapConfiguration getTemplateConfig() {
        return getManagementConfig();
    }

    protected MapConfiguration getGraphConfig() {
        final Map<String, Object> map = getTemplateConfig().getMap();
        map.put(GRAPH_NAME.toStringWithoutRoot(), "inmemory_test_graph_name");
        return ConfigurationUtil.loadMapConfiguration(map);
    }
}


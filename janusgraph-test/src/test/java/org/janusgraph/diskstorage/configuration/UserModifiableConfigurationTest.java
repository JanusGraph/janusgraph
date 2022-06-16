// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.diskstorage.configuration;

import org.apache.commons.configuration2.BaseConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UserModifiableConfigurationTest {

    private UserModifiableConfiguration newBaseConfiguration() {
        final BaseConfiguration base = ConfigurationUtil.createBaseConfiguration();
        final CommonsConfiguration config = new CommonsConfiguration(base);
        return new UserModifiableConfiguration(new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, config.copy(), BasicConfiguration.Restriction.NONE));
    }

    @Test
    public void testRemove() {
        UserModifiableConfiguration configuration = newBaseConfiguration();
        configuration.set("storage.backend", "inmemory");
        assertEquals("inmemory", configuration.get("storage.backend"));

        configuration.set("storage.backend", null);
        assertEquals("null", configuration.get("storage.backend"));

        configuration.set("index.search.backend", "lucene");
        assertEquals("lucene", configuration.get("index.search.backend"));
        assertEquals("+ search\n", configuration.get("index"));

        configuration.set("index.search.backend", null);
        assertEquals("", configuration.get("index"));
    }
}

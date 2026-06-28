// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.graphdb.configuration;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CdcIndexConfigTest {

    @Test
    public void cdcOptionsDefaultAndScopePerIndex() {
        ModifiableConfiguration cfg = GraphDatabaseConfiguration.buildGraphConfiguration();
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_ENABLED, true, "search");
        cfg.set(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS, false, "search");

        Configuration search = cfg.restrictTo("search");
        assertTrue(search.get(GraphDatabaseConfiguration.INDEX_CDC_ENABLED),
            "explicitly enabled for 'search'");
        assertFalse(search.get(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS),
            "explicitly set to cdc-only for 'search'");

        // An index that was never configured keeps the defaults: disabled, and synchronous.
        Configuration other = cfg.restrictTo("other");
        assertFalse(other.get(GraphDatabaseConfiguration.INDEX_CDC_ENABLED),
            "default is disabled");
        assertTrue(other.get(GraphDatabaseConfiguration.INDEX_CDC_SYNCHRONOUS),
            "default is synchronous (dual when enabled)");
    }
}

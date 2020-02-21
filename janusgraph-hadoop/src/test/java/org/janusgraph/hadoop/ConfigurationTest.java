// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.hadoop.config.JanusGraphHadoopConfiguration;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConfigurationTest {
    @Test
    public void testIncludeRoot() {
        Configuration config = new Configuration();
        config.setBoolean("janusgraphmr.ioformat.filter-partitioned-vertices", true);

        ModifiableHadoopConfiguration scanConf = ModifiableHadoopConfiguration.of(JanusGraphHadoopConfiguration.MAPRED_NS, config);

        assertEquals(ConfigElement.getPath(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES), "ioformat.filter-partitioned-vertices");
        assertEquals(ConfigElement.getPath(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES, true), "janusgraphmr.ioformat.filter-partitioned-vertices");

        assertFalse(scanConf.has(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES));
        assertTrue(scanConf.has(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES, true));

        assertFalse(scanConf.get(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES));
        assertTrue(scanConf.get(JanusGraphHadoopConfiguration.FILTER_PARTITIONED_VERTICES, true));
    }
}

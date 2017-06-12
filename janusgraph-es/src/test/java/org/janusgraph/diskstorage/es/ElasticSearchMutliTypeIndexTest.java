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

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.BULK_REFRESH;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.INTERFACE;
import static org.janusgraph.diskstorage.es.ElasticSearchIndex.USE_DEPRECATED_MULTITYPE_INDEX;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;

/**
 * @author David Clement (david.clement90@laposte.net)
 */

public class ElasticSearchMutliTypeIndexTest extends ElasticSearchIndexTest {

    public Configuration getESTestConfig() {
        final String index = "es";
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(INTERFACE, ElasticSearchSetup.REST_CLIENT.toString(), index);
        config.set(INDEX_HOSTS, new String[]{ "127.0.0.1" }, index);
        config.set(BULK_REFRESH, "wait_for", index);
        config.set(USE_DEPRECATED_MULTITYPE_INDEX, true, index);
        return config.restrictTo(index);
    }
}

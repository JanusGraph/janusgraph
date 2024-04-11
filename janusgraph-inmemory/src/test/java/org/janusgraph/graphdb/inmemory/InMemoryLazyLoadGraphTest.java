// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.graphdb.inmemory;

import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.builder.GraphDatabaseConfigurationBuilder;
import org.janusgraph.graphdb.database.LazyLoadGraphTest;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class InMemoryLazyLoadGraphTest extends InMemoryGraphTest {

    @Override
    public void open(WriteConfiguration config) {
        graph = new LazyLoadGraphTest(new GraphDatabaseConfigurationBuilder().build(config));
        features = graph.getConfiguration().getStoreFeatures();
        tx = graph.buildTransaction().start();
        mgmt = graph.openManagement();
    }
}

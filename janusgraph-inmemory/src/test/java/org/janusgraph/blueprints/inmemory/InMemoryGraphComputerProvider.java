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

package org.janusgraph.blueprints.inmemory;

import org.apache.tinkerpop.gremlin.GraphProvider;
import org.janusgraph.StorageSetup;
import org.janusgraph.blueprints.AbstractJanusGraphComputerProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;

/**
 * Created by bryn on 06/05/15.
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class InMemoryGraphComputerProvider extends AbstractJanusGraphComputerProvider {

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        ModifiableConfiguration config = super.getJanusGraphConfiguration(graphName, test, testMethodName);
        config.setAll(StorageSetup.getInMemoryConfiguration().getAll());
        config.set(GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL,false);
        return config;
    }
}

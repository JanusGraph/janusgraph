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

package org.janusgraph.blueprints.thrift;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.blueprints.AbstractJanusGraphComputerProvider;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.graphdb.olap.computer.FulgoraGraphComputer;
import org.apache.tinkerpop.gremlin.GraphProvider;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@GraphProvider.Descriptor(computer = FulgoraGraphComputer.class)
public class ThriftGraphComputerProvider extends AbstractJanusGraphComputerProvider {

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        CassandraStorageSetup.startCleanEmbedded();
        ModifiableConfiguration config = super.getJanusGraphConfiguration(graphName, test, testMethodName);
        config.setAll(CassandraStorageSetup.getCassandraThriftConfiguration(graphName).getAll());
        return config;
    }

}

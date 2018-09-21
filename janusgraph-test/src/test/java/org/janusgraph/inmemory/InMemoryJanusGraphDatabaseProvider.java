// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.inmemory;

import com.google.common.base.Preconditions;
import org.janusgraph.JanusGraphDatabaseProvider;
import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.IDAuthorityTest;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.JanusGraphPartitionGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.olap.OLAPTest;

import java.util.Map;

public class InMemoryJanusGraphDatabaseProvider extends JanusGraphDatabaseProvider {
    //region blueprints
    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return StorageSetup.getInMemoryConfiguration();
    }
    //endregion

    //region diskstorage
    private KeyColumnValueStoreManager sharedKeyColumnValueStoreManager = null;

    @Override
    public KeyColumnValueStoreManager openStorageManager(Object baseTest, String testMethodName, int id, Configuration configuration) throws BackendException {
        if (baseTest instanceof IDAuthorityTest){
            if(sharedKeyColumnValueStoreManager == null) {
                sharedKeyColumnValueStoreManager =  new InMemoryStoreManager();
            }
            return sharedKeyColumnValueStoreManager;
        }
        return new InMemoryStoreManager();
    }

    @Override
    public OrderedKeyValueStoreManager openOrderedStorageManager(Object baseTest, String testMethodName) throws BackendException {
        return null;
    }

    @Override
    public void clopen(KeyColumnValueStoreTest keyColumnValueStoreTest, String methodName) throws BackendException {

    }
    //endregion

    //region graphdb
    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(Object baseTest, String testMethodName) {
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
        if (baseTest instanceof JanusGraphPartitionGraphTest) {
            config.set(GraphDatabaseConfiguration.IDS_FLUSH,false);
        }
        return config;
    }

    @Override
    public void clopen(JanusGraphBaseTest baseTest, Object... settings) {
        if (baseTest instanceof OLAPTest) {
            Preconditions.checkArgument(settings==null || settings.length==0);
        } else
        // noinspection StatementWithEmptyBody
        if (baseTest instanceof JanusGraphPartitionGraphTest) {

        } else if (settings!=null && settings.length>0) {
            if (baseTest.graph!=null && baseTest.graph.isOpen()) {
                Preconditions.checkArgument(!baseTest.graph.vertices().hasNext() &&
                    !baseTest.graph.edges().hasNext(),"Graph cannot be re-initialized for InMemory since that would delete all data");
                baseTest.graph.close();
            }
            Map<JanusGraphBaseTest.TestConfigOption,Object> options = baseTest.validateConfigOptions(settings);
            ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
            config.set(GraphDatabaseConfiguration.STORAGE_BACKEND,"inmemory");
            for (Map.Entry<JanusGraphBaseTest.TestConfigOption,Object> option : options.entrySet()) {
                config.set(option.getKey().option, option.getValue(), option.getKey().umbrella);
            }
            baseTest.open(config.getConfiguration());
        }
        baseTest.newTx();
    }
    //endregion
}

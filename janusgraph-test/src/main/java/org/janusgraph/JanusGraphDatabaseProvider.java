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

package org.janusgraph;

import com.google.common.base.Preconditions;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.graphdb.JanusGraphBaseTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.util.Map;
import java.util.Set;

public abstract class JanusGraphDatabaseProvider {
    //region blueprints
    public abstract ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName);
    public Set<Class> extendImplementations(Set<Class> impls){
        return impls;
    }
    //endregion

    //region diskstorage
    public abstract KeyColumnValueStoreManager openStorageManager(Object baseTest, String testMethodName, int id, Configuration configuration) throws BackendException;
    public abstract OrderedKeyValueStoreManager openOrderedStorageManager(Object baseTest, String testMethodName) throws BackendException;

    public void clopen(KeyColumnValueStoreTest keyColumnValueStoreTest, String methodName) throws BackendException {
        keyColumnValueStoreTest.close();
        keyColumnValueStoreTest.open();
    }
    //endregion

    //region diskstorage
    public abstract ModifiableConfiguration getJanusGraphConfiguration(Object baseTest, String testMethodName);
    public void clopen(JanusGraphBaseTest baseTest, Object... settings) {
        baseTest.config = baseTest.getConfiguration();
        if (baseTest.mgmt!=null && baseTest.mgmt.isOpen()) baseTest.mgmt.rollback();
        if (null != baseTest.tx && baseTest.tx.isOpen()) baseTest.tx.commit();
        if (settings!=null && settings.length>0) {
            final Map<JanusGraphBaseTest.TestConfigOption,Object> options = baseTest.validateConfigOptions(settings);
            JanusGraphManagement janusGraphManagement = null;
            final ModifiableConfiguration modifiableConfiguration = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, baseTest.config, BasicConfiguration.Restriction.LOCAL);
            for (final Map.Entry<JanusGraphBaseTest.TestConfigOption,Object> option : options.entrySet()) {
                if (option.getKey().option.isLocal()) {
                    modifiableConfiguration.set(option.getKey().option,option.getValue(),option.getKey().umbrella);
                } else {
                    if (janusGraphManagement==null) janusGraphManagement = baseTest.graph.openManagement();
                    janusGraphManagement.set(ConfigElement.getPath(option.getKey().option,option.getKey().umbrella),option.getValue());
                }
            }
            if (janusGraphManagement!=null) janusGraphManagement.commit();
            modifiableConfiguration.close();
        }
        if (null != baseTest.graph && null != baseTest.graph.tx() && baseTest.graph.tx().isOpen())
            baseTest.graph.tx().commit();
        if (null != baseTest.graph && baseTest.graph.isOpen())
            baseTest.graph.close();
        Preconditions.checkNotNull(baseTest.config);
        baseTest.open(baseTest.config);
    }
    //endregion
}


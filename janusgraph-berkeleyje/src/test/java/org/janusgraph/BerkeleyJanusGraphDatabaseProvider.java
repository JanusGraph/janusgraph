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

import com.google.common.collect.ImmutableMap;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyFixedLengthKCVSTest;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyJETx;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.io.File;
import java.time.Duration;
import java.util.Set;

public class BerkeleyJanusGraphDatabaseProvider extends JanusGraphDatabaseProvider {
    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(String graphName, Class<?> test, String testMethodName) {
        return BerkeleyStorageSetup.getBerkeleyJEConfiguration(StorageSetup.getHomeDir(graphName))
            .set(GraphDatabaseConfiguration.IDAUTHORITY_WAIT, Duration.ofMillis(150L));
    }

    @Override
    public Set<Class> extendImplementations(Set<Class> implementations) {
        implementations.add(BerkeleyJETx.class);
        return implementations;
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager(Object baseTest, String testMethodName, int id, Configuration configuration) throws BackendException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(getBerkeleyConfig(baseTest, testMethodName));
        if(baseTest instanceof BerkeleyFixedLengthKCVSTest){
            return new OrderedKeyValueStoreManagerAdapter(sm, ImmutableMap.of("testStore1", 8));
        }
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

    @Override
    public OrderedKeyValueStoreManager openOrderedStorageManager(Object baseTest, String testMethodName) throws BackendException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(getBerkeleyConfig(baseTest, testMethodName));
        return sm;
    }

    @Override
    public ModifiableConfiguration getJanusGraphConfiguration(Object baseTest, String testMethodName) {
        if (baseTest instanceof JanusGraphTest) {
            ModifiableConfiguration modifiableConfiguration = getBerkeleyConfig(baseTest, testMethodName);
            // Check that getConfiguration() explicitly set serializable isolation
            // This could be enforced with a JUnit assertion instead of a Precondition,
            // but a failure here indicates a problem in the test itself rather than the
            // system-under-test, so a Precondition seems more appropriate
            if (testMethodName.equals("testConsistencyEnforcement")) {
                BerkeleyJEStoreManager.IsolationLevel iso = BerkeleyJEStoreManager.IsolationLevel.SERIALIZABLE;
                modifiableConfiguration.set(BerkeleyJEStoreManager.ISOLATION_LEVEL, iso.toString());
            } else {
                BerkeleyJEStoreManager.IsolationLevel iso = null;
                if (modifiableConfiguration.has(BerkeleyJEStoreManager.ISOLATION_LEVEL)) {
                    iso = ConfigOption.getEnumValue(modifiableConfiguration.get(BerkeleyJEStoreManager.ISOLATION_LEVEL), BerkeleyJEStoreManager.IsolationLevel.class);
                }
            }
            return modifiableConfiguration;
        }
        return getBerkeleyConfig(baseTest, testMethodName);
    }

    private ModifiableConfiguration getBerkeleyConfig(Object baseTest, String testMethodName) {
        return BerkeleyStorageSetup.getBerkeleyJEConfiguration(StorageSetup.getHomeDir(baseTest.getClass().getName()) + File.separator + testMethodName);
    }
}

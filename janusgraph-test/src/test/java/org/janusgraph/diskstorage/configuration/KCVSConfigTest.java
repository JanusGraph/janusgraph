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

package org.janusgraph.diskstorage.configuration;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.backend.KCVSConfiguration;
import org.janusgraph.diskstorage.inmemory.InMemoryStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.BackendOperation;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSConfigTest extends WritableConfigurationTest {

    @Override
    public WriteConfiguration getConfig() {
        final KeyColumnValueStoreManager manager = new InMemoryStoreManager(Configuration.EMPTY);
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.TIMESTAMP_PROVIDER, TimestampProviders.MICRO);
        try {
            return new KCVSConfiguration(new BackendOperation.TransactionalProvider() {
                @Override
                public StoreTransaction openTx() throws BackendException {
                    return manager.beginTransaction(StandardBaseTransactionConfig.of(TimestampProviders.MICRO, manager.getFeatures().getKeyConsistentTxConfig()));
                }

                @Override
                public void close() throws BackendException {
                    manager.close();
                }
            }, config, manager.openDatabase("janusgraph"),"general");
        } catch (BackendException e) {
            throw new RuntimeException(e);
        }
    }
}

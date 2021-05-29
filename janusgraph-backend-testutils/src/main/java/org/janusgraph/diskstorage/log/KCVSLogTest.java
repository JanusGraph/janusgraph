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

package org.janusgraph.diskstorage.log;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StoreManager;
import org.janusgraph.diskstorage.log.kcvs.KCVSLogManager;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;

import java.time.Duration;

/**
 * Implementation of the {@link LogTest} for {@link KCVSLogManager} based log implementations.
 * This test only requires getting instances of {@link KeyColumnValueStoreManager}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class KCVSLogTest extends LogTest {

    public abstract KeyColumnValueStoreManager openStorageManager() throws BackendException;

    public static final String LOG_NAME = "testlog";

    private KeyColumnValueStoreManager storeManager;

    @Override
    public LogManager openLogManager(String senderId, boolean requiresOrderPreserving) throws BackendException {
        storeManager = openStorageManager();
        ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
        config.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID,senderId);
        config.set(GraphDatabaseConfiguration.LOG_READ_INTERVAL, Duration.ofMillis(500L), LOG_NAME);
        //To ensure that the write order is preserved in reading, we need to ensure that all writes go to the same partition
        //otherwise readers will independently read from the partitions out-of-order by design to avoid having to synchronize
        config.set(KCVSLogManager.LOG_FIXED_PARTITION, requiresOrderPreserving, LOG_NAME);
        return new KCVSLogManager(storeManager,config.restrictTo(LOG_NAME));
    }

    @Override
    @BeforeEach
    public void setup(TestInfo testInfo) throws Exception {
        StoreManager m = openStorageManager();
        m.clearStorage();
        m.close();
        super.setup(testInfo);
    }

    @Override
    @AfterEach
    public void shutdown(TestInfo testInfo) throws Exception {
        super.shutdown(testInfo);
        storeManager.close();
    }

}

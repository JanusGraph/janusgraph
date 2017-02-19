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

package org.janusgraph.diskstorage.hbase;

import java.io.IOException;

import org.janusgraph.diskstorage.BackendException;

import org.apache.hadoop.hbase.util.VersionInfo;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.janusgraph.HBaseStorageSetup;
import org.janusgraph.diskstorage.DistributedStoreManagerTest;


public class HBaseDistributedStoreManagerTest extends DistributedStoreManagerTest<HBaseStoreManager> {

    @BeforeClass
    public static void startHBase() throws IOException {
        HBaseStorageSetup.startHBase();
    }

    @AfterClass
    public static void stopHBase() {
        // Workaround for https://issues.apache.org/jira/browse/HBASE-10312
        if (VersionInfo.getVersion().startsWith("0.96"))
            HBaseStorageSetup.killIfRunning();
    }

    @Before
    public void setUp() throws BackendException {
        manager = new HBaseStoreManager(HBaseStorageSetup.getHBaseConfiguration());
        store = manager.openDatabase("distributedStoreTest");
    }

    @After
    public void tearDown() throws BackendException {
        store.close();
        manager.close();
    }
}

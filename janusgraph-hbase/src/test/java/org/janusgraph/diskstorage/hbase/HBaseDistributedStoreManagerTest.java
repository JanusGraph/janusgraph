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

import org.janusgraph.HBaseContainer;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.DistributedStoreManagerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class HBaseDistributedStoreManagerTest extends DistributedStoreManagerTest<HBaseStoreManager> {
    @Container
    public static final HBaseContainer hBaseContainer = new HBaseContainer();

    @BeforeEach
    public void setUp() throws BackendException {
        manager = new HBaseStoreManager(hBaseContainer.getModifiableConfiguration());
        store = manager.openDatabase("distributedStoreTest");
    }

    @AfterEach
    public void tearDown() throws BackendException {
        store.close();
        manager.close();
    }
}

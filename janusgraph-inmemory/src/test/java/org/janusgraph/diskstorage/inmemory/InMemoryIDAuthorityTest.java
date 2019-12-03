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

package org.janusgraph.diskstorage.inmemory;

import org.janusgraph.diskstorage.IDAuthorityTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class InMemoryIDAuthorityTest extends IDAuthorityTest {

    /**
     * The IDAllocationTest assumes that every StoreManager returned by
     * {@link #openStorageManager()} can see one another's reads and writes. In
     * the HBase and Cassandra tests, we can open a new StoreManager in every
     * call to {@code openStorageManager} and they will all satisfy this
     * constraint, since every manager opens with the same config and talks to
     * the same service. It's not really necessary to have separate managers,
     * but it's nice for getting an extra bit of test coverage. However,
     * separate in-memory managers wouldn't be able to see one another's
     * reads/writes, so we just open a single manager and store it here.
     */
    private final InMemoryStoreManager sharedManager;

    public InMemoryIDAuthorityTest() {
        super();
        sharedManager = new InMemoryStoreManager();
    }

    @Override
    public KeyColumnValueStoreManager openStorageManager() {
        return sharedManager;
    }
}

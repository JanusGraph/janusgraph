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

package org.janusgraph.diskstorage.berkeleyje;

import org.janusgraph.BerkeleyStorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.diskstorage.log.KCVSLogTest;


public class BerkeleyLogTest extends KCVSLogTest {

    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        BerkeleyJEStoreManager sm = new BerkeleyJEStoreManager(BerkeleyStorageSetup.getBerkeleyJEConfiguration());
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }

}

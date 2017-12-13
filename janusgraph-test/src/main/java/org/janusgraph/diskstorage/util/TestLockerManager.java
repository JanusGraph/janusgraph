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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.Locker;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TestLockerManager {

    public static boolean ERROR_ON_LOCKING = true;

    public TestLockerManager() {

    }

    public Locker openLocker(String name) {
        return new TestLocker(name,ERROR_ON_LOCKING);
    }

    private static class TestLocker implements Locker {

        private final boolean errorOnLock;

        private TestLocker(String name, boolean errorOnLock) {
            this.errorOnLock = errorOnLock;
        }

        @Override
        public void writeLock(KeyColumn lockID, StoreTransaction tx) {
            if (errorOnLock)
                throw new UnsupportedOperationException("Locking is not supported!");
        }

        @Override
        public void checkLocks(StoreTransaction tx) {
            //Do nothing since no locks where written
        }

        @Override
        public void deleteLocks(StoreTransaction tx) {
            //Do nothing since no locks where written
        }
    }
}

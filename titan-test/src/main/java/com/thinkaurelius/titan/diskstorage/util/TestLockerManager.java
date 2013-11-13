package com.thinkaurelius.titan.diskstorage.util;

import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;

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
        private final String name;

        private TestLocker(String name, boolean errorOnLock) {
            this.errorOnLock = errorOnLock;
            this.name = name;
        }

        @Override
        public void writeLock(KeyColumn lockID, StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {
            if (errorOnLock) throw new UnsupportedOperationException();
        }

        @Override
        public void checkLocks(StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {
            //Do nothing since no locks where written
        }

        @Override
        public void deleteLocks(StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {
            //Do nothing since no locks where written
        }
    }
}

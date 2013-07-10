package com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.recipes.locks.StaleLockException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.Locker;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLocker.LockState;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;

public class AstyanaxRecipeLocker implements Locker {
    
    private final AstyanaxRecipeLockerConfiguration conf;
    private final LockState<AstyanaxLockStatus> lockState;
    
    private static final Logger log = LoggerFactory.getLogger(AstyanaxRecipeLocker.class);
    
    public AstyanaxRecipeLocker(AstyanaxRecipeLockerConfiguration conf) {
        this.conf = conf;
        this.lockState = new LockState<AstyanaxLockStatus>();
    }

    @Override
    public void writeLock(KeyColumn lockID, StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {
          
        if (lockState.has(tx, lockID)) {
            log.warn("Transaction {} already wrote lock on {}", tx, lockID);
            return;
        }
        
        if (lockLocally(lockID, tx)) {
            boolean ok = false;
            try {
                AstyanaxLockStatus stat = tryLockRemotely(lockID, tx);
                lockLocally(lockID, stat.getTimestamp(TimeUnit.NANOSECONDS), tx); // update local lock expiration time
                lockState.take(tx, lockID, stat);
                ok = true;
            } catch (TemporaryStorageException tse) {
                throw new TemporaryLockingException(tse);
            } catch (AssertionError ae) {
                // Concession to ease testing with mocks & behavior verification
                ok = true;
                throw ae;
            } catch (Throwable t) {
                throw new PermanentLockingException(t);
            } finally {
                if (!ok) {
                    // lockState.release(tx, lockID); // has no effect
                    unlockLocally(lockID, tx);
                }
            }
        } else {
            // Fail immediately with no retries on local contention
            throw new TemporaryLockingException("Local lock contention");
        }
        
    }
    
    protected AstyanaxLockStatus tryLockRemotely(KeyColumn lockID, StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {
        
        long approxTimeNS = conf.getTimes().getApproxNSSinceEpoch(false);
        
        ByteBuffer keyToLock = conf.getSerializer().toLockKey(lockID.getKey(), lockID.getColumn()).asByteBuffer();
        
        ColumnPrefixDistributedRowLock<ByteBuffer> lock =
                new ColumnPrefixDistributedRowLock<ByteBuffer>(
                        conf.getKeyspace(), conf.getColumnFamily(), keyToLock).expireLockAfter(conf.getLockExpire(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS).withConsistencyLevel(ConsistencyLevel.CL_QUORUM);
      
        try {
            lock.acquire();
            return new AstyanaxLockStatus(approxTimeNS, TimeUnit.NANOSECONDS, lock);
        } catch (StaleLockException e) {
            throw new TemporaryLockingException(e); // TODO handle gracefully?
        } catch (BusyLockException e) {
            throw new TemporaryLockingException(e); // TODO handle gracefully?
        } catch (Exception e) {
            throw new PermanentLockingException(e);
        }
    }

    private boolean lockLocally(KeyColumn lockID, StoreTransaction txh) {
        return lockLocally(lockID, conf.getTimes().getApproxNSSinceEpoch(false), txh);
    }
    
    private boolean lockLocally(KeyColumn lockID, long tsNS, StoreTransaction txh) {
        final LocalLockMediator<StoreTransaction> m = conf.getLocalLockMediator();
        return m.lock(lockID, txh, tsNS + conf.getLockExpireNS(), TimeUnit.NANOSECONDS);
    }
    
    private void unlockLocally(KeyColumn lockID, StoreTransaction txh) {
        final LocalLockMediator<StoreTransaction> m = conf.getLocalLockMediator();
        m.unlock(lockID, txh);
    }
    
    @Override
    public void checkLocks(StoreTransaction tx) throws StorageException {
        // Nothing to do
    }

    @Override
    public void deleteLocks(StoreTransaction tx) throws StorageException {
        Map<KeyColumn, AstyanaxLockStatus> m = lockState.getLocksForTx(tx);
        
        Iterator<KeyColumn> iter = m.keySet().iterator();
        while (iter.hasNext()) {
            KeyColumn kc = iter.next();
            AstyanaxLockStatus ls = m.get(kc);
            tryDeleteSingleLock(kc, ls, tx);
            // Regardless of whether we successfully deleted the lock from storage, take it out of the local mediator
            conf.getLocalLockMediator().unlock(kc, tx);
            iter.remove();
        }
    }
    
    protected void tryDeleteSingleLock(KeyColumn lockID, AstyanaxLockStatus stat, StoreTransaction tx) throws PermanentLockingException {
        try {
            stat.getLock().release();
        } catch (Exception e) {
            throw new PermanentLockingException(e); // TODO handle better?
        }
    }
    
    public static class AstyanaxLockStatus {
        private final long time;
        private final TimeUnit timeUnit;
        private final ColumnPrefixDistributedRowLock<ByteBuffer> lock;
        public AstyanaxLockStatus(long time, TimeUnit timeUnit,
                ColumnPrefixDistributedRowLock<ByteBuffer> lock) {
            this.time = time;
            this.timeUnit = timeUnit;
            this.lock = lock;
        }
        
        public long getTimestamp(TimeUnit tu) {
            return tu.convert(time, timeUnit);
        }

        public ColumnPrefixDistributedRowLock<ByteBuffer> getLock() {
            return lock;
        }
    }
}

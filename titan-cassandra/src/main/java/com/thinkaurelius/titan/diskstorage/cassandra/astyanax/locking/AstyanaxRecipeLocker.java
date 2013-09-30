package com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.recipes.locks.BusyLockException;
import com.netflix.astyanax.recipes.locks.ColumnPrefixDistributedRowLock;
import com.netflix.astyanax.recipes.locks.StaleLockException;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.AbstractLocker;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.locking.LockerState;
import com.thinkaurelius.titan.diskstorage.locking.PermanentLockingException;
import com.thinkaurelius.titan.diskstorage.locking.TemporaryLockingException;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

public class AstyanaxRecipeLocker extends AbstractLocker<AstyanaxLockStatus> {

    private final Keyspace lockKeyspace;

    private final ColumnFamily<ByteBuffer, String> lockColumnFamily;

    private static final Logger log = LoggerFactory.getLogger(AstyanaxRecipeLocker.class);

    public static class Builder extends AbstractLocker.Builder<AstyanaxLockStatus, Builder> {

        private Keyspace ks;
        private ColumnFamily<ByteBuffer, String> cf;


        public Builder(Keyspace ks, ColumnFamily<ByteBuffer, String> cf) {
            this.ks = ks;
            this.cf = cf;
        }

        public AstyanaxRecipeLocker build() {
            super.preBuild();
            return new AstyanaxRecipeLocker(rid, times, serializer, llm, lockState, lockExpireNS, log, ks, cf);
        }

        @Override
        protected Builder self() {
            return this;
        }

        @Override
        protected LocalLockMediator<StoreTransaction> getDefaultMediator() {
            return LocalLockMediators.INSTANCE.get(cf.getName());
        }
    }

    private AstyanaxRecipeLocker(StaticBuffer rid, TimestampProvider times,
                                 ConsistentKeyLockerSerializer serializer,
                                 LocalLockMediator<StoreTransaction> llm,
                                 LockerState<AstyanaxLockStatus> lockState, long lockExpireNS,
                                 Logger log, Keyspace lockKeyspace,
                                 ColumnFamily<ByteBuffer, String> lockColumnFamily) {
        super(rid, times, serializer, llm, lockState, lockExpireNS, log);
        this.lockKeyspace = lockKeyspace;
        this.lockColumnFamily = lockColumnFamily;
    }

    @Override
    protected AstyanaxLockStatus writeSingleLock(KeyColumn lockID, StoreTransaction tx) throws TemporaryLockingException, PermanentLockingException {

        long approxTimeNS = times.getApproxNSSinceEpoch();

        ByteBuffer keyToLock = serializer.toLockKey(lockID.getKey(), lockID.getColumn()).asByteBuffer();

        ColumnPrefixDistributedRowLock<ByteBuffer> lock =
                new ColumnPrefixDistributedRowLock<ByteBuffer>(
                        lockKeyspace, lockColumnFamily, keyToLock).expireLockAfter(lockExpireNS, TimeUnit.NANOSECONDS).withConsistencyLevel(ConsistencyLevel.CL_QUORUM);

        try {
            lock.acquire();
            log.debug("Locked {} in store {}", lockID, lockColumnFamily.getName());
            return new AstyanaxLockStatus(approxTimeNS, TimeUnit.NANOSECONDS, lock);
        } catch (StaleLockException e) {
            throw new TemporaryLockingException(e); // TODO handle gracefully?
        } catch (BusyLockException e) {
            throw new TemporaryLockingException(e); // TODO handle gracefully?
        } catch (Exception e) {
            throw new PermanentLockingException(e);
        }
    }

    @Override
    protected void checkSingleLock(
            KeyColumn lockID,
            AstyanaxLockStatus lockStatus,
            StoreTransaction tx) throws Throwable {
        // Nothing to do
    }

    @Override
    protected void deleteSingleLock(KeyColumn lockID, AstyanaxLockStatus stat, StoreTransaction tx) throws PermanentLockingException {
        try {
            stat.getLock().release();
            log.debug("Unlocked {} in store {}", lockID, lockColumnFamily.getName());
        } catch (Exception e) {
            throw new PermanentLockingException(e); // TODO handle better?
        }
    }
}
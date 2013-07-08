package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.BaseConfiguration;

import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class ConsistentKeyLockerConfiguration {
    
    
    /**
     * Storage backend for locking records.
     */
    private final KeyColumnValueStore store;
    
    /**
     * Uniquely identifies a process within a domain (or across all domains,
     * though only intra-domain uniqueness is required)
     */
    private final StaticBuffer rid;
    
    private final TimestampProvider times;
    
    private final ConsistentKeyLockerSerializer serializer;
    
    private final LocalLockMediator<StoreTransaction> llm;

    private final long lockWaitNS;
    
    private final int lockRetryCount;
    
    private final long lockExpireNS;
    
    
    public static class Builder {
        // Required (no default)
        private final KeyColumnValueStore store;
        
        // Optional (has default)
        private StaticBuffer rid;
        private TimestampProvider times;
        private ConsistentKeyLockerSerializer serializer;
        private LocalLockMediator<StoreTransaction> llm;
        private long lockWaitNS;
        private int lockRetryCount;
        private long lockExpireNS;
        
        public Builder(KeyColumnValueStore store) {
            this.store = store;
            this.rid = new StaticByteBuffer(DistributedStoreManager.getRid(new BaseConfiguration()));
            this.times = TimeUtility.INSTANCE;
            this.serializer = new ConsistentKeyLockerSerializer();
            this.llm = null; // redundant, but it preserves this constructor's overall pattern
            this.lockWaitNS = NANOSECONDS.convert(GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT, MILLISECONDS);
            this.lockRetryCount = GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT;
            this.lockExpireNS = NANOSECONDS.convert(GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT, MILLISECONDS);
        }
        
        public Builder rid(StaticBuffer rid) {
            this.rid = rid; return this;
        }

        public Builder times(TimestampProvider times) {
            this.times = times; return this;
        }

        public Builder serializer(ConsistentKeyLockerSerializer serializer) {
            this.serializer = serializer; return this;
        }
        
        public Builder mediator(LocalLockMediator<StoreTransaction> mediator) {
            this.llm = mediator; return this;
        }
        
        public Builder lockWaitNS(long wait, TimeUnit unit) {
            this.lockWaitNS = NANOSECONDS.convert(wait, unit); return this;
        }
        
        public Builder lockRetryCount(int count) {
            this.lockRetryCount = count; return this;
        }
        
        public Builder lockExpireNS(long exp, TimeUnit unit) {
            this.lockExpireNS = NANOSECONDS.convert(exp, unit); return this;
        }
        
        public ConsistentKeyLockerConfiguration build() {
            
            if (null == llm) {
                llm = LocalLockMediators.INSTANCE.get(store.getName());
            }
            
            return new ConsistentKeyLockerConfiguration(store, rid, times, serializer, llm, lockWaitNS, lockRetryCount, lockExpireNS);
        }
    }

    private ConsistentKeyLockerConfiguration(KeyColumnValueStore store,
            StaticBuffer rid, TimestampProvider times,
            ConsistentKeyLockerSerializer serializer, LocalLockMediator<StoreTransaction> llm,
            long lockWaitNS, int lockRetryCount, long lockExpireNS) {
        this.store = store;
        this.rid = rid;
        this.times = times;
        this.serializer = serializer;
        this.llm = llm;
        this.lockWaitNS = lockWaitNS;
        this.lockRetryCount = lockRetryCount;
        this.lockExpireNS = lockExpireNS;
    }

    public KeyColumnValueStore getStore() {
        return store;
    }

    public StaticBuffer getRid() {
        return rid;
    }

    public TimestampProvider getTimes() {
        return times;
    }

    public ConsistentKeyLockerSerializer getSerializer() {
        return serializer;
    }
    
    public LocalLockMediator<StoreTransaction> getLocalLockMediator() {
        return llm;
    }

    public long getLockWaitNS() {
        return lockWaitNS;
    }
    
    public long getLockWait(TimeUnit tu) {
        return tu.convert(lockWaitNS, NANOSECONDS);
    }

    public int getLockRetryCount() {
        return lockRetryCount;
    }

    public long getLockExpireNS() {
        return lockExpireNS;
    }
    
    public long getLockExpire(TimeUnit tu) {
        return tu.convert(lockExpireNS, NANOSECONDS);
    }
}

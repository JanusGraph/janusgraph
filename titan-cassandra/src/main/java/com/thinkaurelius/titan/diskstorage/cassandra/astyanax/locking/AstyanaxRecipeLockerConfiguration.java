package com.thinkaurelius.titan.diskstorage.cassandra.astyanax.locking;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import com.google.common.base.Preconditions;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.ColumnFamily;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockStore;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.LocalLockMediators;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.util.StaticByteBuffer;
import com.thinkaurelius.titan.diskstorage.util.TimeUtility;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;

public class AstyanaxRecipeLockerConfiguration {
    
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

    // Astyanax-specific
    private final Keyspace lockKeyspace;
    private final ColumnFamily<ByteBuffer, String> lockColumnFamily;
    
    public static class Builder {
        
        // Optional (has default)
        private StaticBuffer rid;
        private TimestampProvider times;
        private ConsistentKeyLockerSerializer serializer;
        private LocalLockMediator<StoreTransaction> llm;
        private long lockWaitNS;
        private int lockRetryCount;
        private long lockExpireNS;
        
        private Keyspace lockKeyspace;
        private ColumnFamily<ByteBuffer, String> lockColumnFamily;
        
        public Builder(Keyspace keyspace, ColumnFamily<ByteBuffer, String> cf) {
            this.rid = new StaticByteBuffer(DistributedStoreManager.getRid(new BaseConfiguration()));
            this.times = TimeUtility.INSTANCE;
            this.serializer = new ConsistentKeyLockerSerializer();
            this.llm = null; // redundant, but it preserves this constructor's overall pattern
            this.lockWaitNS = NANOSECONDS.convert(GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT, MILLISECONDS);
            this.lockRetryCount = GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT;
            this.lockExpireNS = NANOSECONDS.convert(GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT, MILLISECONDS);
            this.lockKeyspace = keyspace;
            this.lockColumnFamily = cf;
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
        
        public Builder lockKeyspace(Keyspace ks) {
            this.lockKeyspace = ks; return this;
        }
        
        public Builder lockColumnFamily(ColumnFamily<ByteBuffer, String> cf) {
            this.lockColumnFamily = cf; return this;
        }
        
        public Builder fromCommonsConfig(Configuration config) {
            rid(new StaticArrayBuffer(DistributedStoreManager.getRid(config)));

            final String llmPrefix = config.getString(
                            ConsistentKeyLockStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY,
                            null);
            
            if (null != llmPrefix) {
                mediator(LocalLockMediators.INSTANCE.get(llmPrefix));
            }

            lockRetryCount(config.getInt(
                    GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
                    GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT));

            lockWaitNS(config.getLong(
                    GraphDatabaseConfiguration.LOCK_WAIT_MS,
                    GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT), TimeUnit.MILLISECONDS);

            lockExpireNS(config.getLong(
                    GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
                    GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT), TimeUnit.MILLISECONDS);
            return this;
        }
        
        public Builder mediatorName(String name) {
            Preconditions.checkNotNull(name);
            mediator(LocalLockMediators.INSTANCE.get(name));
            return this;
        }
        
        public AstyanaxRecipeLockerConfiguration build() {
            
            if (null == llm) {
                llm = LocalLockMediators.INSTANCE.get(lockColumnFamily.getName());
            }
            
            return new AstyanaxRecipeLockerConfiguration(rid, times, serializer, llm, lockWaitNS, lockRetryCount, lockExpireNS, lockKeyspace, lockColumnFamily);
        }
    }

    private AstyanaxRecipeLockerConfiguration(
            StaticBuffer rid, TimestampProvider times,
            ConsistentKeyLockerSerializer serializer, LocalLockMediator<StoreTransaction> llm,
            long lockWaitNS, int lockRetryCount, long lockExpireNS, Keyspace ks, ColumnFamily<ByteBuffer, String> cf) {
        this.rid = rid;
        this.times = times;
        this.serializer = serializer;
        this.llm = llm;
        this.lockWaitNS = lockWaitNS;
        this.lockRetryCount = lockRetryCount;
        this.lockExpireNS = lockExpireNS;
        this.lockKeyspace = ks;
        this.lockColumnFamily = cf;
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
    
    public Keyspace getKeyspace() {
        return lockKeyspace;
    }
    
    public ColumnFamily<ByteBuffer, String> getColumnFamily() {
        return lockColumnFamily;
    }
}

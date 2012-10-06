package com.thinkaurelius.titan.diskstorage.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransactionHandle;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;

/**
 * This class overrides and adds nothing compared with
 * {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockTransaction}; however, it creates a transaction type specific
 * to Cassandra, which lets us check for user errors like passing a HBase
 * transaction into a Cassandra method.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraTransaction extends AbstractStoreTransactionHandle {

    public enum Consistency { 
        ONE, TWO, THREE, ANY, ALL, QUORUM, LOCAL_QUORUM, EACH_QUORUM;
        
        public static Consistency parse(String value) {
            Preconditions.checkArgument(value!=null && !value.isEmpty());
            value = value.trim();
            if (value.equals("1")) return ONE;
            else if (value.equals("2")) return TWO;
            else if (value.equals("3")) return THREE;
            else {
                for (Consistency c : values()) {
                    if (c.toString().equalsIgnoreCase(value)) return c;
                }
            }
            throw new IllegalArgumentException("Unrecognized cassandra consistency level: " + value);
        }
    
        
    }

    private final Consistency readConsistency;
    private final Consistency writeConsistency;
    
    public CassandraTransaction(ConsistencyLevel level, Consistency readConsistency, Consistency writeConsistency) {
        super(level);
        if (level== ConsistencyLevel.KEY_CONSISTENT) {
            this.readConsistency=Consistency.QUORUM;
            this.writeConsistency=Consistency.QUORUM;
        } else {
            Preconditions.checkNotNull(readConsistency);
            Preconditions.checkNotNull(writeConsistency);
            this.readConsistency=readConsistency;
            this.writeConsistency=writeConsistency;
        }
    }

    public Consistency getReadConsistencyLevel() {
        return readConsistency;
    }
    
    public Consistency getWriteConsistencyLevel() {
        return writeConsistency;
    }


}

package com.thinkaurelius.titan.diskstorage.cassandra;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * This class overrides and adds nothing compared with
 * {@link com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpectedValueCheckingTransaction}; however, it creates a transaction type specific
 * to Cassandra, which lets us check for user errors like passing a HBase
 * transaction into a Cassandra method.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraTransaction extends AbstractStoreTransaction {

    public enum Consistency {
        ONE, TWO, THREE, ANY, ALL, QUORUM, LOCAL_QUORUM, EACH_QUORUM;

        public static Consistency parse(String value) {
            Preconditions.checkArgument(value != null && !value.isEmpty());
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

        public com.netflix.astyanax.model.ConsistencyLevel getAstyanaxConsistency() {
            switch (this) {
                case ONE:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_ONE;
                case TWO:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_TWO;
                case THREE:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_THREE;
                case ALL:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_ALL;
                case ANY:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_ANY;
                case QUORUM:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_QUORUM;
                case LOCAL_QUORUM:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_LOCAL_QUORUM;
                case EACH_QUORUM:
                    return com.netflix.astyanax.model.ConsistencyLevel.CL_EACH_QUORUM;
                default:
                    throw new IllegalArgumentException("Unrecognized consistency level: " + this);
            }
        }

        public org.apache.cassandra.db.ConsistencyLevel getDBConsistency() {
            org.apache.cassandra.db.ConsistencyLevel level = org.apache.cassandra.db.ConsistencyLevel.valueOf(this.toString());
            Preconditions.checkArgument(level!=null);
            return level;
        }

        public org.apache.cassandra.thrift.ConsistencyLevel getThriftConsistency() {
            org.apache.cassandra.thrift.ConsistencyLevel level = org.apache.cassandra.thrift.ConsistencyLevel.valueOf(this.toString());
            Preconditions.checkArgument(level != null);
            return level;
        }
    }

    private final Consistency readConsistency;
    private final Consistency writeConsistency;

    public CassandraTransaction(ConsistencyLevel level, Consistency readConsistency, Consistency writeConsistency) {
        super(level);
        if (level == ConsistencyLevel.KEY_CONSISTENT) {
            this.readConsistency = Consistency.QUORUM;
            this.writeConsistency = Consistency.QUORUM;
        } else {
            Preconditions.checkNotNull(readConsistency);
            Preconditions.checkNotNull(writeConsistency);
            this.readConsistency = readConsistency;
            this.writeConsistency = writeConsistency;
        }
    }

    public Consistency getReadConsistencyLevel() {
        return readConsistency;
    }

    public Consistency getWriteConsistencyLevel() {
        return writeConsistency;
    }

    public static CassandraTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null && (txh instanceof CassandraTransaction));
        return (CassandraTransaction) txh;
    }


}

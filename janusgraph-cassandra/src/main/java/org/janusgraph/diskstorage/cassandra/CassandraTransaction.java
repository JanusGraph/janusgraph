package org.janusgraph.diskstorage.cassandra;

import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

public class CassandraTransaction extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(CassandraTransaction.class);

    private final CLevel read;
    private final CLevel write;

    public CassandraTransaction(BaseTransactionConfig c) {
        super(c);
        read =  CLevel.parse(getConfiguration().getCustomOption(CASSANDRA_READ_CONSISTENCY));
        write = CLevel.parse(getConfiguration().getCustomOption(CASSANDRA_WRITE_CONSISTENCY));
        log.debug("Created {}", this.toString());
    }

    public CLevel getReadConsistencyLevel() {
        return read;
    }

    public CLevel getWriteConsistencyLevel() {
        return write;
    }

    public static CassandraTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        Preconditions.checkArgument(txh instanceof CassandraTransaction, "Unexpected transaction type %s", txh.getClass().getName());
        return (CassandraTransaction) txh;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(64);
        sb.append("CassandraTransaction@");
        sb.append(Integer.toHexString(hashCode()));
        sb.append("[read=");
        sb.append(read);
        sb.append(",write=");
        sb.append(write);
        sb.append("]");
        return sb.toString();
    }
}

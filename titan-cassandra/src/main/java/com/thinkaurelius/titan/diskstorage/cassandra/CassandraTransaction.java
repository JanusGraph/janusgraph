package com.thinkaurelius.titan.diskstorage.cassandra;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_READ_CONSISTENCY;
import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.CASSANDRA_WRITE_CONSISTENCY;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.AbstractStoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

public class CassandraTransaction extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(CassandraTransaction.class);

    public CassandraTransaction(BaseTransactionConfig c) {
        super(c);
    }

    public CLevel getReadConsistencyLevel() {
        CLevel lev = CLevel.parse(getConfiguration().getCustomOption(CASSANDRA_READ_CONSISTENCY));
        log.debug("Read consistency level for tx {} is {}", this, lev);
        return lev;
    }

    public CLevel getWriteConsistencyLevel() {
        CLevel lev = CLevel.parse(getConfiguration().getCustomOption(CASSANDRA_WRITE_CONSISTENCY));
        log.debug("Write consistency level for tx {} is {}", this, lev);
        return lev;
    }

    public static CassandraTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        Preconditions.checkArgument(txh instanceof CassandraTransaction, "Unexpected transaction type %s", txh.getClass().getName());
        return (CassandraTransaction) txh;
    }
}

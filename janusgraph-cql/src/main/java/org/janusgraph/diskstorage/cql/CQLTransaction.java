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

package org.janusgraph.diskstorage.cql;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.READ_CONSISTENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.WRITE_CONSISTENCY;

/**
 * This class manages the translation of read and write consistency configuration values to CQL API {@link ConsistencyLevel} types.
 */
public class CQLTransaction extends AbstractStoreTransaction {

    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    public CQLTransaction(final BaseTransactionConfig config) {
        super(config);
        this.readConsistencyLevel = DefaultConsistencyLevel.valueOf(getConfiguration().getCustomOption(READ_CONSISTENCY));
        this.writeConsistencyLevel = DefaultConsistencyLevel.valueOf(getConfiguration().getCustomOption(WRITE_CONSISTENCY));
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return this.readConsistencyLevel;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return this.writeConsistencyLevel;
    }

    public static CQLTransaction getTransaction(final StoreTransaction storeTransaction) {
        Preconditions.checkNotNull(storeTransaction);
        Preconditions.checkArgument(storeTransaction instanceof CQLTransaction, "Unexpected transaction type %s", storeTransaction.getClass().getName());
        return (CQLTransaction) storeTransaction;
    }

    @Override
    public String toString() {
        return "CQLTransaction@" + Integer.toHexString(hashCode()) + "[read=" + this.readConsistencyLevel
            + ",write=" + this.writeConsistencyLevel + "]";
    }
}

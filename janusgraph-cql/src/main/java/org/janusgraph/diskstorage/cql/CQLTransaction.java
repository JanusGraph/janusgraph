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

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.*;

import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import com.datastax.driver.core.ConsistencyLevel;
import com.google.common.base.Preconditions;

/**
 * This class manages the translation of read and write consistency configuration values to CQL API {@link ConsistencyLevel} types.
 */
public class CQLTransaction extends AbstractStoreTransaction {

    private final ConsistencyLevel readConsistencyLevel;
    private final ConsistencyLevel writeConsistencyLevel;

    public CQLTransaction(final BaseTransactionConfig config) {
        super(config);
        this.readConsistencyLevel = ConsistencyLevel.valueOf(getConfiguration().getCustomOption(READ_CONSISTENCY));
        this.writeConsistencyLevel = ConsistencyLevel.valueOf(getConfiguration().getCustomOption(WRITE_CONSISTENCY));
    }

    ConsistencyLevel getReadConsistencyLevel() {
        return this.readConsistencyLevel;
    }

    ConsistencyLevel getWriteConsistencyLevel() {
        return this.writeConsistencyLevel;
    }

    static CQLTransaction getTransaction(final StoreTransaction storeTransaction) {
        Preconditions.checkArgument(storeTransaction != null);
        Preconditions.checkArgument(storeTransaction instanceof CQLTransaction, "Unexpected transaction type %s", storeTransaction.getClass().getName());
        return (CQLTransaction) storeTransaction;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(64);
        sb.append("CQLTransaction@");
        sb.append(Integer.toHexString(hashCode()));
        sb.append("[read=");
        sb.append(this.readConsistencyLevel);
        sb.append(",write=");
        sb.append(this.writeConsistencyLevel);
        sb.append("]");
        return sb.toString();
    }
}

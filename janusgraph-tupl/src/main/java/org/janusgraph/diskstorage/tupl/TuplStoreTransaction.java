/*
 * Copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.janusgraph.diskstorage.tupl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import org.cojen.tupl.*;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.locking.PermanentLockingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class TuplStoreTransaction extends AbstractStoreTransaction {
    private static final Logger log = LoggerFactory.getLogger(TuplStoreTransaction.class);
    private static final String HEX_PREFIX = "0x";
    private final String id;
    private final Transaction txn;
    private final Database database;
    private final Map<StaticBuffer, StaticBuffer> expectedValues;

    TuplStoreTransaction(BaseTransactionConfig config, Transaction txn, Database database) {
        super(config);
        this.txn = txn;
        this.database = database;
        this.expectedValues = new HashMap<>();
        id = HEX_PREFIX + Long.toHexString(System.nanoTime());
        log.debug("begin id:{} config:{}", id, config);
    }

    static TuplStoreTransaction getTx(StoreTransaction txh) {
        Preconditions.checkArgument(txh != null);
        Preconditions.checkArgument(txh instanceof TuplStoreTransaction,
                        "Unexpected transaction type %s", txh.getClass().getName());
        return (TuplStoreTransaction) txh;
    }

    Transaction getTuplTxn() {
        return txn;
    }

    @VisibleForTesting
    void expectValue(String name, long indexId, StaticBuffer key, StaticBuffer expectedValue) throws PermanentLockingException {
        log.trace("expectValue id:{} index:{} name:{} key:{} expected:{}", id, indexId, name, key, expectedValue);
        if (expectedValues.containsKey(key)) {
            return; //TODO is this correct?
        }
        expectedValues.put(key, expectedValue);

    }

    @VisibleForTesting
    StaticBuffer getExpectedValue(String name, long indexId, StaticBuffer key) {
        log.trace("get expected value id:{} index:{} name:{} key:{}", id, indexId, name, key);
        return expectedValues.get(key);
    }

    @Override
    public void commit() throws BackendException {
        log.debug("commit txn={}, id={}", txn, id);
        try {
            txn.commit();
            txn.exit();
            if (DurabilityMode.NO_REDO == txn.durabilityMode()) { //TODO should I always call checkpoint?
                database.checkpoint();
            }
        } catch (IOException e) {
            throw new PermanentBackendException("unable to commit tx " + id, e);
        }
    }

    @Override
    public void rollback() throws BackendException {
        log.debug("rollback txn={}, id={}", txn, id);
        expectedValues.clear();
        try {
            txn.reset();
        } catch (IOException e) {
            throw new PermanentBackendException("unable to commit tx " + id, e);
        }
    }

    public String toString() {
        return String.format("TuplStoreTransaction id=%s", id);
    }

    String getId() {
        return id;
    }
}

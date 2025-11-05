/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.couchbase.client.java.Cluster;

import java.util.HashMap;
import java.util.Map;

public class CouchbaseTx extends AbstractStoreTransaction {

    private static final Logger log = LoggerFactory.getLogger(CouchbaseTx.class);

    private final Cluster cluster;
    private final Map<String, Map<String, Long>> locks = new HashMap<>();

    public CouchbaseTx(Cluster cluster, BaseTransactionConfig config) {
        super(config);
        this.cluster = cluster;
        log.trace("Created new transaction");
    }

    @Override
    public void commit() throws BackendException {
        super.commit();
    }

    public void addLock(String table, String key, Long cas) {
        if (!locks.containsKey(table)) {
            locks.put(table, new HashMap<>());
        }
        Map<String, Long> tableLocks = locks.get(table);
        if (!tableLocks.containsKey(key)) {
            tableLocks.put(key, cas);
        }
    }

    public Long getLock(String table, String key) {
        if (locks.containsKey(table)) {
            return locks.get(table).get(key);
        }
        return null;
    }
}

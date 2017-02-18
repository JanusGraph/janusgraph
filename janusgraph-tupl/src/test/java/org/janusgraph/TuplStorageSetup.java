/*
 * Copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
 * Portions copyright Titan: Distributed Graph Database - Copyright 2012 and onwards Aurelius.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.janusgraph;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_BACKEND;

import org.cojen.tupl.LockMode;
import org.janusgraph.diskstorage.tupl.TuplStoreManager;

import org.janusgraph.StorageSetup;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

/**
 * Utility methods to create graph configurations
 * @author Alexander Patrikalakis
 *
 */
public class TuplStorageSetup extends StorageSetup {
    public static ModifiableConfiguration getTuplGraphBaseConfiguration(String name) {
        // 8gb was not enough for testVertexCentricQuery so use 16g for max cache size and 16mb for min cache size
        // TODO lock mode needs some investigation
        return buildGraphConfiguration().set(STORAGE_BACKEND, "tupl")
                                        .set(GraphDatabaseConfiguration.STORAGE_DIRECTORY, getHomeDir(name))
                                        .set(TuplStoreManager.TUPL_MIN_CACHE_SIZE, 16777216L)
                                        .set(TuplStoreManager.TUPL_MAX_CACHE_SIZE, 17179869184L)
                                        .set(TuplStoreManager.TUPL_LOCK_MODE, LockMode.READ_COMMITTED.name())
                                        .set(TuplStoreManager.TUPL_LOCK_TIMEOUT, 1L);
    }
    public static KeyColumnValueStoreManager getKCVStorageManager(String name) throws BackendException {
        final TuplStoreManager sm = new TuplStoreManager(getTuplGraphBaseConfiguration(name));
        return new OrderedKeyValueStoreManagerAdapter(sm);
    }
}

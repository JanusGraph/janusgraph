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
package org.janusgraph.diskstorage.tupl;

import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import java.util.concurrent.ExecutionException;

import org.janusgraph.TuplStorageSetup;
import org.janusgraph.diskstorage.tupl.TuplStoreManager;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.keyvalue.OrderedKeyValueStoreManagerAdapter;
import org.junit.rules.TestName;

/**
 * @author Alexander Patrikalakis
 *
 */
public class TuplFixedLengthKCVSTest extends KeyColumnValueStoreTest {

    @Rule
    public TestName testName = new TestName();

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        final TuplStoreManager sm =
                new TuplStoreManager(TuplStorageSetup.getTuplGraphBaseConfiguration("TuplFixedLengthKCVSTest#"
                        + testName.getMethodName()));
        return new OrderedKeyValueStoreManagerAdapter(sm, ImmutableMap.of(storeName, 8));

    }

    @Test @Override
    public void testConcurrentGetSliceAndMutate() throws BackendException, ExecutionException, InterruptedException {

    }
}

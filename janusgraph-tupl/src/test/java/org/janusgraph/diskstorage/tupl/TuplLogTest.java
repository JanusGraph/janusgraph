/*
 * Copyright 2016 Classmethod, Inc. or its affiliates. All Rights Reserved.
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

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.log.KCVSLogTest;

import org.janusgraph.TuplStorageSetup;

/**
 * Log tests for the Tupl KV Store
 * @author Alexander Patrikalakis
 *
 */
public class TuplLogTest extends KCVSLogTest {

    @Override
    public KeyColumnValueStoreManager openStorageManager() throws BackendException {
        return TuplStorageSetup.getKCVStorageManager("TuplLogTest#" + testName.getMethodName());
    }

}

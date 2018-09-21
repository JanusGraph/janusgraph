// Copyright 2018 JanusGraph Authors
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

package org.janusgraph;

import org.janusgraph.diskstorage.IDAuthorityTest;
import org.janusgraph.diskstorage.KeyColumnValueStoreTest;
import org.janusgraph.diskstorage.KeyValueStoreTest;
import org.janusgraph.diskstorage.LockKeyColumnValueStoreTest;
import org.janusgraph.diskstorage.log.KCVSLogTest;
import org.janusgraph.graphdb.*;
import org.janusgraph.graphdb.database.management.ManagementTest;
import org.janusgraph.olap.OLAPTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class JanusGraphBackendTestSuite extends AbstractJanusGraphTestSuite {

    private static final Class<?>[] allTests = new Class<?>[]{
        KCVSLogTest.class,
        KeyColumnValueStoreTest.class,
        KeyValueStoreTest.class,
        LockKeyColumnValueStoreTest.class,
        IDAuthorityTest.class,
        JanusGraphTest.class,
        JanusGraphPartitionGraphTest.class,
        JanusGraphOperationCountingTest.class,
        JanusGraphIoTest.class,
        JanusGraphEventualGraphTest.class,
        JanusGraphConcurrentTest.class,
        OLAPTest.class,
        JanusGraphPerformanceMemoryTest.class,
        ManagementTest.class
    };

    public JanusGraphBackendTestSuite(Class<?> klass, RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests);
    }
}

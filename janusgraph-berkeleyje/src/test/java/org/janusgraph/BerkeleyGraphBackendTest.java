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

import org.janusgraph.diskstorage.berkeleyje.BerkeleyFixedLengthKCVSTest;
import org.janusgraph.diskstorage.berkeleyje.BerkeleyVariableLengthKCVSTest;
import org.janusgraph.graphdb.berkeleyje.BerkeleyGraphTest;
import org.junit.runner.RunWith;

@RunWith(JanusGraphBackendTestSuite.class)
@JanusGraphProviderClass(provider = BerkeleyJanusGraphDatabaseProvider.class)
@JanusGraphSpecificTestClass(testClass = BerkeleyFixedLengthKCVSTest.class)
@JanusGraphSpecificTestClass(testClass = BerkeleyVariableLengthKCVSTest.class)
@JanusGraphSpecificTestClass(testClass = BerkeleyGraphTest.class)

@JanusGraphIgnoreTest(
    test = "org.janusgraph.graphdb.berkeleyje.BerkeleyGraphTest",
    method = "testConcurrentConsistencyEnforcement",
    reason = "Do nothing TODO: Figure out why this is failing in BerkeleyDB!!")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.berkeleyje.BerkeleyFixedLengthKCVSTest", method = "testConcurrentGetSlice", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.berkeleyje.BerkeleyFixedLengthKCVSTest", method = "testConcurrentGetSliceAndMutate", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.berkeleyje.BerkeleyVariableLengthKCVSTest", method = "testConcurrentGetSlice", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.berkeleyje.BerkeleyVariableLengthKCVSTest", method = "testConcurrentGetSliceAndMutate", reason = "TODO")

@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.KeyColumnValueStoreTest", method = "*", reason = "Executed with BerkeleyVariableLengthKCVSTest and BerkeleyFixedLengthKCVSTest")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphTest", method = "*", reason = "Executed with BerkeleyGraphTest")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.LockKeyColumnValueStoreTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.diskstorage.IDAuthorityTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphIoTest", method = "*", reason = "TODO")
@JanusGraphIgnoreTest(test = "org.janusgraph.graphdb.JanusGraphEventualGraphTest", method = "*", reason = "TODO")
public class BerkeleyGraphBackendTest {
}


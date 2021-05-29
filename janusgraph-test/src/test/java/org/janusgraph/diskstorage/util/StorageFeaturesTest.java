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

package org.janusgraph.diskstorage.util;

import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StorageFeaturesTest {

    @Test
    public void testFeaturesImplementation() {
        StoreFeatures features;

        features = new StandardStoreFeatures.Builder().build();

        assertFalse(features.hasMultiQuery());
        assertFalse(features.hasLocking());
        assertFalse(features.isDistributed());
        assertFalse(features.hasScan());

        features = new StandardStoreFeatures.Builder().locking(true).build();

        assertFalse(features.hasMultiQuery());
        assertTrue(features.hasLocking());
        assertFalse(features.isDistributed());

        features = new StandardStoreFeatures.Builder().multiQuery(true).unorderedScan(true).build();

        assertTrue(features.hasMultiQuery());
        assertTrue(features.hasUnorderedScan());
        assertFalse(features.hasOrderedScan());
        assertTrue(features.hasScan());
        assertFalse(features.isDistributed());
        assertFalse(features.hasLocking());

        features = new StandardStoreFeatures.Builder().multiQuery(true).orderedScan(true).build();

        assertTrue(features.hasMultiQuery());
        assertFalse(features.hasUnorderedScan());
        assertTrue(features.hasOrderedScan());
        assertTrue(features.hasScan());
        assertFalse(features.isDistributed());
        assertFalse(features.hasLocking());
    }


}

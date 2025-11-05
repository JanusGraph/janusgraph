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

package org.janusgraph.diskstorage.couchbase.mocks;

import com.couchbase.client.java.Collection;
import com.couchbase.client.java.manager.collection.CollectionManager;
import org.mockito.Mockito;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

public class CollectionManagerMock {
    private static final Map<String, Collection> COLLECTIONS = new HashMap<>();
    public static final Map<String, Collection> collections() {
        return COLLECTIONS;
    }

    private static WeakReference<CollectionManager> MOCK;
    public static CollectionManager get() {
        if (MOCK == null || MOCK.get() == null) {
            CollectionManager collectionManager = Mockito.mock(CollectionManager.class);
            MOCK = new WeakReference<>(collectionManager);
        }
        return MOCK.get();
    }
}

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

import com.couchbase.client.java.manager.query.QueryIndexManager;

import static org.mockito.Mockito.mock;

public class QueryIndexManagerMock {
    private static QueryIndexManager MOCK;
    public static QueryIndexManager get() {
        if (MOCK == null) {
            MOCK = mock(QueryIndexManager.class);
        }
        return MOCK;
    }

    public static void reset() {
        MOCK = null;
    }
}

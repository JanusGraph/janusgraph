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

import com.couchbase.client.java.Scope;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScopeMock {
    private static Scope MOCK;
    public static Scope get() {
        if (MOCK == null) {
            Scope scope = mock(Scope.class);
            when(scope.name()).thenReturn(ClusterMock.SCOPE);
            MOCK = scope;
        }
        return MOCK;
    }

    public static void reset() {
        MOCK = null;
    }
}

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

import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.couchbase.CouchbaseIndexConfigOptions;
import org.mockito.Mockito;

import java.lang.ref.WeakReference;

public class ConfigMock {

    private static WeakReference<Configuration> MOCK;

    public static Configuration get() {
        if (MOCK == null || MOCK.get() == null) {
            Configuration config = Mockito.mock(Configuration.class);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_STRING)).thenReturn(ClusterMock.ADDRESS);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_USERNAME)).thenReturn(ClusterMock.USER);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_PASSWORD)).thenReturn(ClusterMock.PASSWORD);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_CONNECT_BUCKET)).thenReturn(ClusterMock.BUCKET);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_SCOPE)).thenReturn(ClusterMock.SCOPE);
            Mockito.when(config.get(CouchbaseIndexConfigOptions.CLUSTER_DEFAULT_FUZINESS)).thenReturn(2);
            MOCK = new WeakReference<>(config);
        }
        return MOCK.get();
    }
}

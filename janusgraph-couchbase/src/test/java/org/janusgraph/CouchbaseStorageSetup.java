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

package org.janusgraph;

import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.buildGraphConfiguration;

public class CouchbaseStorageSetup extends StorageSetup {

    public static ModifiableConfiguration getModifiableCouchbaseConfiguration() {
        return  buildGraphConfiguration()
                .set(GraphDatabaseConfiguration.STORAGE_BACKEND,"org.janusgraph.diskstorage.couchbase.CouchbaseStoreManager")
                .set(GraphDatabaseConfiguration.AUTH_USERNAME, "Administrator")
                .set(GraphDatabaseConfiguration.AUTH_PASSWORD, "password")
                ;
    }

    public static WriteConfiguration getCouchbaseConfiguration() {

        return getModifiableCouchbaseConfiguration().getConfiguration();

    }

}

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

package org.janusgraph.diskstorage.couchbase;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the Couchbase storage backend.
 * These are managed under the 'cb' namespace in the configuration.
 *
 * @author Jagadesh Munta (jagadesh.munta@couchbase.com)
 */
@PreInitializeConfigOptions
public interface CouchbaseConfigOptions {

    ConfigNamespace CB_NS = new ConfigNamespace(
        GraphDatabaseConfiguration.STORAGE_NS,
        "cb",
        "Couchbase storage backend options");

    ConfigOption<Integer> VERSION = new ConfigOption<>(
        CB_NS,
        "version",
        "The version of the Couchbase cluster.",
        ConfigOption.Type.LOCAL,
        703);

    ConfigOption<String> CLUSTER_CONNECT_STRING = new ConfigOption<>(
        CB_NS,
        "cluster-connect-string",
        "Connect string to the Couchbase cluster",
        ConfigOption.Type.LOCAL,
        "couchbase://localhost");

    ConfigOption<String> CLUSTER_CONNECT_USERNAME = new ConfigOption<>(
        CB_NS,
        "cluster-connect-username",
        "Username to the Couchbase cluster",
        ConfigOption.Type.LOCAL,
        "Administrator");

    ConfigOption<String> CLUSTER_CONNECT_PASSWORD = new ConfigOption<>(
        CB_NS,
        "cluster-connect-password",
        "Password to the Couchbase cluster",
        ConfigOption.Type.LOCAL,
        "password");
    
    ConfigOption<String> CLUSTER_CONNECT_BUCKET = new ConfigOption<>(
        CB_NS,
        "cluster-connect-bucket",
        "Bucket in the Couchbase cluster",
        ConfigOption.Type.LOCAL,
        "default");
    
    ConfigOption<String> CLUSTER_DEFAULT_SCOPE = new ConfigOption<>(
        CB_NS,
        "cluster-default-scope",
        "Default Scope ",
        ConfigOption.Type.LOCAL,
        "_default");
    
    ConfigOption<String> CLUSTER_DEFAULT_COLLECTION = new ConfigOption<>(
        CB_NS,
        "cluster-default-collection",
        "Default Collection",
        ConfigOption.Type.LOCAL,
        "_default");
        
    ConfigOption<String> ISOLATION_LEVEL = new ConfigOption<>(
        CB_NS,
        "isolation-level",
        "Options are serializable, read_committed_no_write, read_committed_with_write",
        ConfigOption.Type.LOCAL,
        "serializable");

    ConfigOption<String> GET_RANGE_MODE = new ConfigOption<>(
        CB_NS,
        "get-range-mode",
        "The mod of executing CB getRange, either `iterator` or `list`",
        ConfigOption.Type.LOCAL,
        "list"
    );

}

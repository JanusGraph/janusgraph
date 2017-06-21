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

package org.janusgraph.diskstorage.cql;

import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

/**
 * Configuration options for the CQL storage backend. These are managed under the 'cql' namespace in the configuration.
 */
@PreInitializeConfigOptions
public interface CQLConfigOptions {

    public static final ConfigNamespace CQL_NS = new ConfigNamespace(
            GraphDatabaseConfiguration.STORAGE_NS,
            "cql",
            "CQL storage backend options");

    public static final ConfigOption<String> KEYSPACE = new ConfigOption<>(
            CQL_NS,
            "keyspace",
            "The name of JanusGraph's keyspace.  It will be created if it does not exist.",
            ConfigOption.Type.LOCAL,
            "janusgraph");

    public static final ConfigOption<Integer> PROTOCOL_VERSION = new ConfigOption<>(
            CQL_NS,
            "protocol-version",
            "The protocol version used to connect to the Cassandra database.  If no value is supplied then the driver will negotiate with the server.",
            ConfigOption.Type.LOCAL,
            0);

    public static final ConfigOption<String> READ_CONSISTENCY = new ConfigOption<>(
            CQL_NS,
            "read-consistency-level",
            "The consistency level of read operations against Cassandra",
            ConfigOption.Type.MASKABLE,
            CQLStoreManager.CONSISTENCY_QUORUM);

    public static final ConfigOption<String> WRITE_CONSISTENCY = new ConfigOption<>(
            CQL_NS,
            "write-consistency-level",
            "The consistency level of write operations against Cassandra",
            ConfigOption.Type.MASKABLE,
            CQLStoreManager.CONSISTENCY_QUORUM);

    public static final ConfigOption<Boolean> ONLY_USE_LOCAL_CONSISTENCY_FOR_SYSTEM_OPERATIONS =
        new ConfigOption<Boolean>(CQL_NS, "only-use-local-consistency-for-system-operations",
            "True to prevent any system queries from using QUORUM consistency " +
                "and always use LOCAL_QUORUM instead",
            ConfigOption.Type.MASKABLE, false);

    // The number of statements in a batch
    public static final ConfigOption<Integer> BATCH_STATEMENT_SIZE = new ConfigOption<>(
            CQL_NS,
            "batch-statement-size",
            "The number of statements in each batch",
            ConfigOption.Type.MASKABLE,
            20);

    // Whether to use unlogged batches
    public static final ConfigOption<Boolean> ATOMIC_BATCH_MUTATE = new ConfigOption<>(
            CQL_NS,
            "atomic-batch-mutate",
            "True to use Cassandra atomic batch mutation, false to use non-atomic batches",
            ConfigOption.Type.MASKABLE,
            false);

    // Replication
    public static final ConfigOption<Integer> REPLICATION_FACTOR = new ConfigOption<>(
            CQL_NS,
            "replication-factor",
            "The number of data replicas (including the original copy) that should be kept",
            ConfigOption.Type.GLOBAL_OFFLINE,
            1);

    public static final ConfigOption<String> REPLICATION_STRATEGY = new ConfigOption<>(
            CQL_NS,
            "replication-strategy-class",
            "The replication strategy to use for JanusGraph keyspace",
            ConfigOption.Type.FIXED,
            "SimpleStrategy");

    public static final ConfigOption<String[]> REPLICATION_OPTIONS = new ConfigOption<>(
            CQL_NS,
            "replication-strategy-options",
            "Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a " +
                    "map.  It must have an even number of elements in [key,val,key,val,...] form.  A replication_factor set " +
                    "here takes precedence over one set with " + ConfigElement.getPath(REPLICATION_FACTOR),
            ConfigOption.Type.FIXED,
            String[].class);

    public static final ConfigOption<String> COMPACTION_STRATEGY = new ConfigOption<>(
            CQL_NS,
            "compaction-strategy-class",
            "The compaction strategy to use for JanusGraph tables",
            ConfigOption.Type.FIXED,
            String.class);

    public static final ConfigOption<String[]> COMPACTION_OPTIONS = new ConfigOption<>(
            CQL_NS,
            "compaction-strategy-options",
            "Compaction strategy options.  This list is interpreted as a " +
                    "map.  It must have an even number of elements in [key,val,key,val,...] form.",
            ConfigOption.Type.FIXED,
            String[].class);

    // Compression
    public static final ConfigOption<Boolean> CF_COMPRESSION = new ConfigOption<>(
            CQL_NS,
            "compression",
            "Whether the storage backend should use compression when storing the data",
            ConfigOption.Type.FIXED,
            true);

    public static final ConfigOption<String> CF_COMPRESSION_TYPE = new ConfigOption<>(
            CQL_NS,
            "compression-type",
            "The sstable_compression value JanusGraph uses when creating column families. " +
                    "This accepts any value allowed by Cassandra's sstable_compression option. " +
                    "Leave this unset to disable sstable_compression on JanusGraph-created CFs.",
            ConfigOption.Type.MASKABLE,
            "LZ4Compressor");

    public static final ConfigOption<Integer> CF_COMPRESSION_BLOCK_SIZE = new ConfigOption<>(
            CQL_NS,
            "compression-block-size",
            "The size of the compression blocks in kilobytes",
            ConfigOption.Type.FIXED,
            64);

    // SSL
    public static final ConfigNamespace SSL_NS = new ConfigNamespace(
            CQL_NS,
            "ssl",
            "Configuration options for SSL");

    public static final ConfigNamespace SSL_TRUSTSTORE_NS = new ConfigNamespace(
            SSL_NS,
            "truststore",
            "Configuration options for SSL Truststore.");

    public static final ConfigOption<Boolean> SSL_ENABLED = new ConfigOption<>(
            SSL_NS,
            "enabled",
            "Controls use of the SSL connection to Cassandra",
            ConfigOption.Type.LOCAL,
            false);

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION = new ConfigOption<>(
            SSL_TRUSTSTORE_NS,
            "location",
            "Marks the location of the SSL Truststore.",
            ConfigOption.Type.LOCAL,
            "");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD = new ConfigOption<>(
            SSL_TRUSTSTORE_NS,
            "password",
            "The password to access SSL Truststore.",
            ConfigOption.Type.LOCAL,
            "");

    // Other options
    public static final ConfigOption<String> CLUSTER_NAME = new ConfigOption<>(
            CQL_NS,
            "cluster-name",
            "Default name for the Cassandra cluster",
            ConfigOption.Type.MASKABLE,
            "JanusGraph Cluster");

    public static final ConfigOption<String> LOCAL_DATACENTER = new ConfigOption<>(
            CQL_NS,
            "local-datacenter",
            "The name of the local or closest Cassandra datacenter.  When set and not whitespace, " +
                    "this value will be passed into ConnectionPoolConfigurationImpl.setLocalDatacenter. " +
                    "When unset or set to whitespace, setLocalDatacenter will not be invoked.",
            /*
             * It's between either LOCAL or MASKABLE. MASKABLE could be useful for cases where all the JanusGraph instances are closest to
             * the same Cassandra DC.
             */
            ConfigOption.Type.MASKABLE,
            String.class);

}

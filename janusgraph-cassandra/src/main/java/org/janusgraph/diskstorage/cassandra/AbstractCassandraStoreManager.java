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

package org.janusgraph.diskstorage.cassandra;

import java.util.*;

import com.google.common.collect.ImmutableMap;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.common.DistributedStoreManager;
import org.janusgraph.diskstorage.configuration.ConfigElement;
import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.PreInitializeConfigOptions;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.*;

import org.apache.cassandra.dht.IPartitioner;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@PreInitializeConfigOptions
public abstract class AbstractCassandraStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    public enum Partitioner {

        RANDOM, BYTEORDER;

        public static Partitioner getPartitioner(IPartitioner partitioner) {
            return getPartitioner(partitioner.getClass().getSimpleName());
        }

        public static Partitioner getPartitioner(String className) {
            if (className.endsWith("RandomPartitioner") || className.endsWith("Murmur3Partitioner"))
                return Partitioner.RANDOM;
            else if (className.endsWith("ByteOrderedPartitioner")) return Partitioner.BYTEORDER;
            else throw new IllegalArgumentException("Unsupported partitioner: " + className);
        }
    }

    //################### CASSANDRA SPECIFIC CONFIGURATION OPTIONS ######################

    public static final ConfigNamespace CASSANDRA_NS =
            new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "cassandra", "Cassandra storage backend options");

    public static final ConfigOption<String> CASSANDRA_KEYSPACE =
            new ConfigOption<String>(CASSANDRA_NS, "keyspace",
                    "The name of JanusGraph's keyspace.  It will be created if it does not exist.",
                    ConfigOption.Type.LOCAL, "janusgraph");

    // Consistency Levels and Atomic Batch
    public static final ConfigOption<String> CASSANDRA_READ_CONSISTENCY =
            new ConfigOption<String>(CASSANDRA_NS, "read-consistency-level",
            "The consistency level of read operations against Cassandra",
            ConfigOption.Type.MASKABLE, "QUORUM");

    public static final ConfigOption<String> CASSANDRA_WRITE_CONSISTENCY =
            new ConfigOption<String>(CASSANDRA_NS, "write-consistency-level",
            "The consistency level of write operations against Cassandra",
            ConfigOption.Type.MASKABLE, "QUORUM");

    public static final ConfigOption<Boolean> ATOMIC_BATCH_MUTATE =
            new ConfigOption<Boolean>(CASSANDRA_NS, "atomic-batch-mutate",
            "True to use Cassandra atomic batch mutation, false to use non-atomic batches",
            ConfigOption.Type.MASKABLE, true);

    // Replication
    public static final ConfigOption<Integer> REPLICATION_FACTOR =
            new ConfigOption<Integer>(CASSANDRA_NS, "replication-factor",
            "The number of data replicas (including the original copy) that should be kept. " +
                    "This is only meaningful for storage backends that natively support data replication.",
            ConfigOption.Type.GLOBAL_OFFLINE, 1);

    public static final ConfigOption<String> REPLICATION_STRATEGY =
            new ConfigOption<String>(CASSANDRA_NS, "replication-strategy-class",
            "The replication strategy to use for JanusGraph keyspace",
            ConfigOption.Type.FIXED, "org.apache.cassandra.locator.SimpleStrategy");

    public static final ConfigOption<String[]> REPLICATION_OPTIONS =
            new ConfigOption<String[]>(CASSANDRA_NS, "replication-strategy-options",
            "Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a " +
            "map.  It must have an even number of elements in [key,val,key,val,...] form.  A replication_factor set " +
            "here takes precedence over one set with " + ConfigElement.getPath(REPLICATION_FACTOR),
            ConfigOption.Type.FIXED, String[].class);

    public static final ConfigOption<String> COMPACTION_STRATEGY =
            new ConfigOption<String>(CASSANDRA_NS, "compaction-strategy-class",
            "The compaction strategy to use for JanusGraph tables",
            ConfigOption.Type.FIXED, String.class);

    public static final ConfigOption<String[]> COMPACTION_OPTIONS =
            new ConfigOption<String[]>(CASSANDRA_NS, "compaction-strategy-options",
            "Compaction strategy options.  This list is interpreted as a " +
            "map.  It must have an even number of elements in [key,val,key,val,...] form.",
            ConfigOption.Type.FIXED, String[].class);

    // Compression
    public static final ConfigOption<Boolean> CF_COMPRESSION =
            new ConfigOption<Boolean>(CASSANDRA_NS, "compression",
            "Whether the storage backend should use compression when storing the data", ConfigOption.Type.FIXED, true);

    public static final ConfigOption<String> CF_COMPRESSION_TYPE =
            new ConfigOption<String>(CASSANDRA_NS, "compression-type",
            "The sstable_compression value JanusGraph uses when creating column families. " +
            "This accepts any value allowed by Cassandra's sstable_compression option. " +
            "Leave this unset to disable sstable_compression on JanusGraph-created CFs.",
            ConfigOption.Type.MASKABLE, "LZ4Compressor");

    public static final ConfigOption<Integer> CF_COMPRESSION_BLOCK_SIZE =
            new ConfigOption<Integer>(CASSANDRA_NS, "compression-block-size",
            "The size of the compression blocks in kilobytes", ConfigOption.Type.FIXED, 64);

    // SSL
    public static final ConfigNamespace SSL_NS =
            new ConfigNamespace(CASSANDRA_NS, "ssl", "Configuration options for SSL");

    public static final ConfigNamespace SSL_TRUSTSTORE_NS =
            new ConfigNamespace(SSL_NS, "truststore", "Configuration options for SSL Truststore.");

    public static final ConfigOption<Boolean> SSL_ENABLED =
            new ConfigOption<Boolean>(SSL_NS, "enabled",
            "Controls use of the SSL connection to Cassandra", ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION =
            new ConfigOption<String>(SSL_TRUSTSTORE_NS, "location",
            "Marks the location of the SSL Truststore.", ConfigOption.Type.LOCAL, "");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD =
            new ConfigOption<String>(SSL_TRUSTSTORE_NS, "password",
            "The password to access SSL Truststore.", ConfigOption.Type.LOCAL, "");

    // Thrift transport
    public static final ConfigOption<Integer> THRIFT_FRAME_SIZE_MB =
            new ConfigOption<>(CASSANDRA_NS, "frame-size-mb",
            "The thrift frame size in megabytes", ConfigOption.Type.MASKABLE, 15);

    /**
     * The default Thrift port used by Cassandra. Set
     * {@link GraphDatabaseConfiguration#STORAGE_PORT} to override.
     * <p>
     * Value = {@value}
     */
    public static final int PORT_DEFAULT = 9160;

    public static final String SYSTEM_KS = "system";

    protected final String keySpaceName;
    protected final Map<String, String> strategyOptions;
    protected final Map<String, String> compactionOptions;

    protected final boolean compressionEnabled;
    protected final int compressionChunkSizeKB;
    protected final String compressionClass;

    protected final boolean atomicBatch;

    protected final int thriftFrameSizeBytes;

    private volatile StoreFeatures features = null;
    private Partitioner partitioner = null;

    public AbstractCassandraStoreManager(Configuration config) {
        super(config, PORT_DEFAULT);

        this.keySpaceName = config.get(CASSANDRA_KEYSPACE);
        this.compressionEnabled = config.get(CF_COMPRESSION);
        this.compressionChunkSizeKB = config.get(CF_COMPRESSION_BLOCK_SIZE);
        this.compressionClass = config.get(CF_COMPRESSION_TYPE);
        this.atomicBatch = config.get(ATOMIC_BATCH_MUTATE);
        this.thriftFrameSizeBytes = config.get(THRIFT_FRAME_SIZE_MB) * 1024 * 1024;

        // SSL truststore location sanity check
        if (config.get(SSL_ENABLED) && config.get(SSL_TRUSTSTORE_LOCATION).isEmpty())
            throw new IllegalArgumentException(SSL_TRUSTSTORE_LOCATION.getName() + " could not be empty when SSL is enabled.");

        if (config.has(REPLICATION_OPTIONS)) {
            String[] options = config.get(REPLICATION_OPTIONS);

            if (options.length % 2 != 0)
                throw new IllegalArgumentException(REPLICATION_OPTIONS.getName() + " should have even number of elements.");

            Map<String, String> converted = new HashMap<String, String>(options.length / 2);

            for (int i = 0; i < options.length; i += 2) {
                converted.put(options[i], options[i + 1]);
            }

            this.strategyOptions = ImmutableMap.copyOf(converted);
        } else {
            this.strategyOptions = ImmutableMap.of("replication_factor", String.valueOf(config.get(REPLICATION_FACTOR)));
        }

        if (config.has(COMPACTION_OPTIONS)) {
            String[] options = config.get(COMPACTION_OPTIONS);

            if (options.length % 2 != 0)
                throw new IllegalArgumentException(COMPACTION_OPTIONS.getName() + " should have even number of elements.");

            Map<String, String> converted = new HashMap<String, String>(options.length / 2);

            for (int i = 0; i < options.length; i += 2) {
                converted.put(options[i], options[i + 1]);
            }

            this.compactionOptions = ImmutableMap.copyOf(converted);
        } else {
            this.compactionOptions = ImmutableMap.of();
        }
    }

    public final Partitioner getPartitioner() {
        if (partitioner == null) {
            try {
                partitioner = Partitioner.getPartitioner(getCassandraPartitioner());
            } catch (BackendException e) {
                throw new JanusGraphException("Could not connect to Cassandra to read partitioner information. Please check the connection", e);
            }
        }
        assert partitioner != null;
        return partitioner;
    }

    public abstract IPartitioner getCassandraPartitioner() throws BackendException;

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) {
        return new CassandraTransaction(config);
    }

    @Override
    public String toString() {
        return "[" + keySpaceName + "@" + super.toString() + "]";
    }

    @Override
    public StoreFeatures getFeatures() {

        if (features == null) {

            Configuration global = GraphDatabaseConfiguration.buildGraphConfiguration()
                    .set(CASSANDRA_READ_CONSISTENCY, "QUORUM")
                    .set(CASSANDRA_WRITE_CONSISTENCY, "QUORUM")
                    .set(METRICS_PREFIX, GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT);

            Configuration local = GraphDatabaseConfiguration.buildGraphConfiguration()
                    .set(CASSANDRA_READ_CONSISTENCY, "LOCAL_QUORUM")
                    .set(CASSANDRA_WRITE_CONSISTENCY, "LOCAL_QUORUM")
                    .set(METRICS_PREFIX, GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT);

            StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

            fb.batchMutation(true).distributed(true);
            fb.timestamps(true).cellTTL(true);
            fb.keyConsistent(global, local);
            fb.optimisticLocking(true);

            boolean keyOrdered;

            switch (getPartitioner()) {
                case RANDOM:
                    keyOrdered = false;
                    fb.keyOrdered(keyOrdered).orderedScan(false).unorderedScan(true);
                    break;

                case BYTEORDER:
                    keyOrdered = true;
                    fb.keyOrdered(keyOrdered).orderedScan(true).unorderedScan(false);
                    break;

                default:
                    throw new IllegalArgumentException("Unrecognized partitioner: " + getPartitioner());
            }

            switch (getDeployment()) {
                case REMOTE:
                    fb.multiQuery(true);
                    break;

                case LOCAL:
                    fb.multiQuery(true).localKeyPartition(keyOrdered);
                    break;

                case EMBEDDED:
                    fb.multiQuery(false).localKeyPartition(keyOrdered);
                    break;

                default:
                    throw new IllegalArgumentException("Unrecognized deployment mode: " + getDeployment());
            }

            features = fb.build();
        }

        return features;
    }

    /**
     * Returns a map of compression options for the column family {@code cf}.
     * The contents of the returned map must be identical to the contents of the
     * map returned by
     * {@link org.apache.cassandra.thrift.CfDef#getCompression_options()}, even
     * for implementations of this method that don't use Thrift.
     *
     * @param cf the name of the column family for which to return compression
     *           options
     * @return map of compression option names to compression option values
     * @throws org.janusgraph.diskstorage.BackendException if reading from Cassandra fails
     */
    public abstract Map<String, String> getCompressionOptions(String cf) throws BackendException;

    public String getName() {
        return getClass().getSimpleName() + keySpaceName;
    }

}

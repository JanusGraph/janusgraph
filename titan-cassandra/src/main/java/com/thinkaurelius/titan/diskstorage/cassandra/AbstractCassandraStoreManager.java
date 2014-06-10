package com.thinkaurelius.titan.diskstorage.cassandra;

import java.util.*;

import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.BaseTransactionConfig;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@PreInitializeConfigOptions
public abstract class AbstractCassandraStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    public enum Partitioner {

        RANDOM, BYTEORDER;

        public static Partitioner getPartitioner(IPartitioner<?> partitioner) {
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
    public static final ConfigOption<String> CASSANDRA_READ_CONSISTENCY = new ConfigOption<String>(STORAGE_NS,"read-consistency-level",
            "The consistency level of read operations against Cassandra",
            ConfigOption.Type.MASKABLE, "QUORUM");

//    public static final String READ_CONSISTENCY_LEVEL_KEY = "read-consistency-level";
//    public static final String READ_CONSISTENCY_LEVEL_DEFAULT = "QUORUM";

    public static final ConfigOption<String> CASSANDRA_WRITE_CONSISTENCY = new ConfigOption<String>(STORAGE_NS,"write-consistency-level",
            "The consistency level of write operations against Cassandra",
            ConfigOption.Type.MASKABLE, "QUORUM");

//    public static final String WRITE_CONSISTENCY_LEVEL_KEY = "write-consistency-level";

    /**
     * THRIFT_FRAME_SIZE_IN_MB should be appropriately set when server-side Thrift counterpart was changed,
     * because otherwise client wouldn't be able to accept read/write frames from server as incorrectly sized.
     * <p/>
     * HEADS UP: setting max message size proved itself hazardous to be set on the client, only server needs that
     * kind of protection.
     * <p/>
     * Note: property is sized in megabytes for user convenience (defaults are 15MB by cassandra.yaml).
     */
    public static final ConfigOption<Integer> CASSANDRA_THRIFT_FRAME_SIZE = new ConfigOption<Integer>(STORAGE_NS,"thrift-frame-size",
            "The thrift frame size in mega bytes",
            ConfigOption.Type.MASKABLE, 15);

//    public static final String THRIFT_FRAME_SIZE_MB = "cassandra.thrift.frame_size_mb";

//    /**
//     * This flag would be checked on first Titan run when Keyspace and CFs required
//     * for operation are created. If this flag is set to "true" Snappy
//     * compression mechanism would be used.  Default is "true" (see DEFAULT_COMPRESSION_FLAG).
//     */
//    public static final String ENABLE_COMPRESSION_KEY = "compression.enabled";
//    public static final boolean DEFAULT_COMPRESSION_FLAG = true;
//
//    /**
//     * This property allows to set appropriate initial compression chunk_size (in kilobytes) when compression is enabled,
//     * Default: 64 (see DEFAULT_COMPRESSION_CHUNK_SIZE), should be positive 2^n.
//     */
//    public static final String COMPRESSION_CHUNKS_SIZE_KEY = "compression.chunk_length_kb";
//    public static final int DEFAULT_COMPRESSION_CHUNK_SIZE = 64;

    /**
     * Controls the Cassandra sstable_compression for CFs created by Titan.
     * <p>
     * If a CF already exists, then Titan will not modify its compressor
     * configuration. Put another way, this setting only affects a CF that Titan
     * created because it didn't already exist.
     * <p>
     * Default: {@literal #DEFAULT_COMPRESSOR}
     */
    public static final ConfigOption<String> CASSANDRA_COMPRESSION_TYPE = new ConfigOption<String>(STORAGE_NS,"compression-type",
            "The sstable_compression value to use when creating Titan's column families. " +
            "This accepts any value allowed by Cassandra's sstable_compression option. " +
            "Leave this unset to disable sstable_compression on Titan-created CFs.",
            ConfigOption.Type.MASKABLE, "LZ4Compressor");

    public static final ConfigOption<String> REPLICATION_STRATEGY = new ConfigOption<String>(STORAGE_NS, "replication-strategy-class",
            "The replication strategy to use for Titan keyspace",
            ConfigOption.Type.FIXED, "org.apache.cassandra.locator.SimpleStrategy");

    public static final ConfigOption<List<String>> REPLICATION_OPTIONS = new ConfigOption<List<String>>(STORAGE_NS, "replication-strategy-options",
            "Replication strategy options, e.g. factor or replicas per datacenter.  This list is interpreted as a " +
            "map.  It must have an even number of elements in [key,val,key,val,...] form.  A replication_factor set " +
            "here takes precedence over one set with " + ConfigElement.getPath(REPLICATION_FACTOR),
            ConfigOption.Type.FIXED, new ArrayList<String>(0));

//    public static final String COMPRESSION_KEY = "compression.sstable_compression";
//    public static final String DEFAULT_COMPRESSION = "SnappyCompressor";
//

    public static final ConfigOption<String> CASSANDRA_KEYSPACE = new ConfigOption<String>(STORAGE_NS,"keyspace",
            "The name of Titan's keyspace.  It will be created if it does not exist.",
            ConfigOption.Type.LOCAL, "titan");

    /**
     * The default Thrift port used by Cassandra. Set
     * {@link GraphDatabaseConfiguration#STORAGE_PORT} to override.
     * <p>
     * Value = {@value}
     */
    public static final int PORT_DEFAULT = 9160;

    public static final String SYSTEM_KS = "system";

//    public static final String REPLICATION_FACTOR_KEY = "replication-factor";
//    public static final int REPLICATION_FACTOR_DEFAULT = 1;


    public static final ConfigOption<Boolean> SSL_ENABLED = new ConfigOption<Boolean>(STORAGE_SSL_NS, "enabled",
            "Controls use of the SSL connection to Cassandra", ConfigOption.Type.LOCAL, false);

    public static final ConfigOption<String> SSL_TRUSTSTORE_LOCATION = new ConfigOption<String>(STORAGE_SSL_TRUSTSTORE, "location",
            "Marks the location of the SSL Truststore.", ConfigOption.Type.LOCAL, "");

    public static final ConfigOption<String> SSL_TRUSTSTORE_PASSWORD = new ConfigOption<String>(STORAGE_SSL_TRUSTSTORE, "password",
            "The password to access SSL Truststore.", ConfigOption.Type.LOCAL, "");

    protected final String keySpaceName;
    protected final Map<String, String> strategyOptions;

    // see description for THRIFT_FRAME_SIZE and THRIFT_MAX_MESSAGE_SIZE for details
    protected final int thriftFrameSize;

    private volatile StoreFeatures features = null;
    private Partitioner partitioner = null;

    protected final boolean compressionEnabled;
    protected final int compressionChunkSizeKB;
    protected final String compressionClass;

    public AbstractCassandraStoreManager(Configuration config) {
        super(config, PORT_DEFAULT);

        this.keySpaceName = config.get(CASSANDRA_KEYSPACE);
        this.thriftFrameSize = config.get(CASSANDRA_THRIFT_FRAME_SIZE) * 1024 * 1024;
        this.compressionEnabled = config.get(STORAGE_COMPRESSION);
        this.compressionChunkSizeKB = config.get(STORAGE_COMPRESSION_SIZE);
        this.compressionClass = config.get(CASSANDRA_COMPRESSION_TYPE);

        // SSL truststore location sanity check
        if (config.get(SSL_ENABLED) && config.get(SSL_TRUSTSTORE_LOCATION).isEmpty())
            throw new IllegalArgumentException(SSL_TRUSTSTORE_LOCATION.getName() + " could not be empty when SSL is enabled.");

        if (config.has(REPLICATION_OPTIONS)) {
            List<String> options = config.get(REPLICATION_OPTIONS);
            if (options.size() % 2 != 0)
                throw new IllegalArgumentException(REPLICATION_OPTIONS.getName() + " should have even number of elements.");

            Map<String, String> converted = new HashMap<String, String>(options.size() / 2);

            for (int i = 0; i < options.size(); i += 2) {
                converted.put(options.get(i), options.get(i + 1));
            }

            this.strategyOptions = ImmutableMap.copyOf(converted);
        } else {
            this.strategyOptions = ImmutableMap.of("replication_factor", String.valueOf(config.get(REPLICATION_FACTOR)));
        }
    }

    public final Partitioner getPartitioner() {
        if (partitioner == null) {
            try {
                partitioner = Partitioner.getPartitioner(getCassandraPartitioner());
            } catch (StorageException e) {
                throw new TitanException("Could not connect to Cassandra to read partitioner information. Please check the connection", e);
            }
        }
        assert partitioner != null;
        return partitioner;
    }

    public abstract IPartitioner<? extends Token<?>> getCassandraPartitioner() throws StorageException;

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

            Configuration global = GraphDatabaseConfiguration.buildConfiguration()
                    .set(CASSANDRA_READ_CONSISTENCY, "QUORUM")
                    .set(CASSANDRA_WRITE_CONSISTENCY, "QUORUM")
                    .set(METRICS_PREFIX, GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT);

            Configuration local = GraphDatabaseConfiguration.buildConfiguration()
                    .set(CASSANDRA_READ_CONSISTENCY, "LOCAL_QUORUM")
                    .set(CASSANDRA_WRITE_CONSISTENCY, "LOCAL_QUORUM")
                    .set(METRICS_PREFIX, GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT);

            StandardStoreFeatures.Builder fb = new StandardStoreFeatures.Builder();

            fb.batchMutation(true).distributed(true);
            fb.timestamps(true).ttl(true);
            fb.keyConsistent(global, local);

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
     * @throws StorageException if reading from Cassandra fails
     */
    public abstract Map<String, String> getCompressionOptions(String cf) throws StorageException;

    public String getName() {
        return getClass().getSimpleName() + keySpaceName;
    }

}

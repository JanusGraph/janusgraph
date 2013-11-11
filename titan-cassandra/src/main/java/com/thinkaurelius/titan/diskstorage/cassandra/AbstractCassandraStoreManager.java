package com.thinkaurelius.titan.diskstorage.cassandra;

import java.util.Map;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.commons.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

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
    public static final String READ_CONSISTENCY_LEVEL_KEY = "read-consistency-level";
    public static final String READ_CONSISTENCY_LEVEL_DEFAULT = "QUORUM";

    public static final String WRITE_CONSISTENCY_LEVEL_KEY = "write-consistency-level";

    /**
     * THRIFT_FRAME_SIZE_IN_MB should be appropriately set when server-side Thrift counterpart was changed,
     * because otherwise client wouldn't be able to accept read/write frames from server as incorrectly sized.
     * <p/>
     * HEADS UP: setting max message size proved itself hazardous to be set on the client, only server needs that
     * kind of protection.
     * <p/>
     * Note: property is sized in megabytes for user convenience (defaults are 15MB by cassandra.yaml).
     */
    public static final String THRIFT_FRAME_SIZE_MB = "cassandra.thrift.frame_size_mb";

    /**
     * This flag would be checked on first Titan run when Keyspace and CFs required
     * for operation are created. If this flag is set to "true" Snappy
     * compression mechanism would be used.  Default is "true" (see DEFAULT_COMPRESSION_FLAG).
     */
    public static final String ENABLE_COMPRESSION_KEY = "compression.enabled";
    public static final boolean DEFAULT_COMPRESSION_FLAG = true;

    /**
     * This property allows to set appropriate initial compression chunk_size (in kilobytes) when compression is enabled,
     * Default: 64 (see DEFAULT_COMPRESSION_CHUNK_SIZE), should be positive 2^n.
     */
    public static final String COMPRESSION_CHUNKS_SIZE_KEY = "compression.chunk_length_kb";
    public static final int DEFAULT_COMPRESSION_CHUNK_SIZE = 64;

    /**
     * Controls the Cassandra sstable_compression for CFs created by Titan.
     * <p/>
     * If a CF already exists, then Titan will not modify its compressor
     * configuration. Put another way, this setting only affects a CF that Titan
     * created because it didn't already exist.
     * <p/>
     * Default: {@literal #DEFAULT_COMPRESSOR}
     */
    public static final String COMPRESSION_KEY = "compression.sstable_compression";
    public static final String DEFAULT_COMPRESSION = "SnappyCompressor";


    public static final int THRIFT_DEFAULT_FRAME_SIZE = 15;

    /*
     * Any operation attempted with ConsistencyLevel.TWO
     * against a single-node Cassandra cluster (like the one
     * we use in a lot of our test cases) will fail with
     * an UnavailableException.  In other words, if you
     * set TWO here, Cassandra will require TWO nodes, even
     * if only one node has ever been a member of the
     * cluster in question.
     */
    public static final String WRITE_CONSISTENCY_LEVEL_DEFAULT = "QUORUM";
    /**
     * Default name for the Cassandra keyspace
     * <p/>
     * Value = {@value}
     */
    public static final String KEYSPACE_DEFAULT = "titan";
    public static final String KEYSPACE_KEY = "keyspace";

    /**
     * Default port at which to attempt Cassandra Thrift connection.
     * <p/>
     * Value = {@value}
     */
    public static final int PORT_DEFAULT = 9160;


    public static final String REPLICATION_FACTOR_KEY = "replication-factor";
    public static final int REPLICATION_FACTOR_DEFAULT = 1;


    protected final String keySpaceName;
    protected final int replicationFactor;

    private final CassandraTransaction.Consistency readConsistencyLevel;
    private final CassandraTransaction.Consistency writeConsistencyLevel;

    // see description for THRIFT_FRAME_SIZE and THRIFT_MAX_MESSAGE_SIZE for details
    protected final int thriftFrameSize;

    private StoreFeatures features = null;
    private Partitioner partitioner = null;

    protected final boolean compressionEnabled;
    protected final int compressionChunkSizeKB;
    protected final String compressionClass;


    public AbstractCassandraStoreManager(Configuration storageConfig) {
        super(storageConfig, PORT_DEFAULT);

        this.keySpaceName = storageConfig.getString(KEYSPACE_KEY, KEYSPACE_DEFAULT);

        this.replicationFactor = storageConfig.getInt(REPLICATION_FACTOR_KEY, REPLICATION_FACTOR_DEFAULT);

        this.readConsistencyLevel = CassandraTransaction.Consistency.parse(storageConfig.getString(
                READ_CONSISTENCY_LEVEL_KEY, READ_CONSISTENCY_LEVEL_DEFAULT));

        this.writeConsistencyLevel = CassandraTransaction.Consistency.parse(storageConfig.getString(
                WRITE_CONSISTENCY_LEVEL_KEY, WRITE_CONSISTENCY_LEVEL_DEFAULT));

        this.thriftFrameSize = storageConfig.getInt(THRIFT_FRAME_SIZE_MB, THRIFT_DEFAULT_FRAME_SIZE) * 1024 * 1024;

        this.compressionEnabled = storageConfig.getBoolean(ENABLE_COMPRESSION_KEY, DEFAULT_COMPRESSION_FLAG);
        this.compressionChunkSizeKB = storageConfig.getInt(COMPRESSION_CHUNKS_SIZE_KEY, DEFAULT_COMPRESSION_CHUNK_SIZE);
        this.compressionClass = storageConfig.getString(COMPRESSION_KEY, DEFAULT_COMPRESSION);
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

    public abstract Deployment getDeployment();

    @Override
    public StoreTransaction beginTransaction(final StoreTxConfig config) {
        return new CassandraTransaction(config, readConsistencyLevel, writeConsistencyLevel);
    }

    @Override
    public String toString() {
        return "[" + keySpaceName + "@" + super.toString() + "]";
    }

    @Override
    public StoreFeatures getFeatures() {
        if (features == null) {
            features = new StoreFeatures();
            features.supportsBatchMutation = true;
            features.supportsTransactions = false;
            features.supportsConsistentKeyOperations = true;
            features.supportsLocking = false;
            features.isDistributed = true;

            switch (getPartitioner()) {
                case RANDOM:
                    features.isKeyOrdered = false;
                    features.supportsOrderedScan = false;
                    features.supportsUnorderedScan = true;
                    break;

                case BYTEORDER:
                    features.isKeyOrdered = true;
                    features.supportsOrderedScan = true;
                    features.supportsUnorderedScan = false;
                    break;

                default:
                    throw new IllegalArgumentException("Unrecognized partitioner: " + getPartitioner());
            }

            switch (getDeployment()) {
                case REMOTE:
                    features.supportsMultiQuery = true;
                    features.hasLocalKeyPartition = false;
                    break;

                case LOCAL:
                    features.supportsMultiQuery = true;
                    features.hasLocalKeyPartition = features.isKeyOrdered;
                    break;

                case EMBEDDED:
                    features.supportsMultiQuery = false;
                    features.hasLocalKeyPartition = features.isKeyOrdered;
                    break;

                default:
                    throw new IllegalArgumentException("Unrecognized deployment mode: " + getDeployment());
            }
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

package com.thinkaurelius.titan.diskstorage.cassandra;

import java.util.Map;

import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ConsistencyLevel;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.commons.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractCassandraStoreManager extends DistributedStoreManager implements KeyColumnValueStoreManager {

    public enum Partitioner {

        RANDOM, BYTEORDER, LOCALBYTEORDER;

        public static Partitioner getPartitioner(IPartitioner<?> partitioner) {
            return getPartitioner(partitioner.getClass().getSimpleName());
        }

        public static Partitioner getPartitioner(String className) {
            if (className.endsWith("RandomPartitioner") || className.endsWith("Murmur3Partitioner")) return Partitioner.RANDOM;
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
     *
     * HEADS UP: setting max message size proved itself hazardous to be set on the client, only server needs that
     * kind of protection.
     *
     * Note: property is sized in megabytes for user convenience (defaults are 15MB by cassandra.yaml).
     */
    public static final String THRIFT_FRAME_SIZE_MB = "cassandra.thrift.frame_size_mb";

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

    protected static final String SYSTEM_PROPERTIES_CF  = "system_properties";
    protected static final String SYSTEM_PROPERTIES_KEY = "general";

    public AbstractCassandraStoreManager(Configuration storageConfig) {
        super(storageConfig, PORT_DEFAULT);

        this.keySpaceName = storageConfig.getString(KEYSPACE_KEY, KEYSPACE_DEFAULT);

        this.replicationFactor = storageConfig.getInt(REPLICATION_FACTOR_KEY, REPLICATION_FACTOR_DEFAULT);

        this.readConsistencyLevel = CassandraTransaction.Consistency.parse(storageConfig.getString(
                READ_CONSISTENCY_LEVEL_KEY, READ_CONSISTENCY_LEVEL_DEFAULT));

        this.writeConsistencyLevel = CassandraTransaction.Consistency.parse(storageConfig.getString(
                WRITE_CONSISTENCY_LEVEL_KEY, WRITE_CONSISTENCY_LEVEL_DEFAULT));

        this.thriftFrameSize = storageConfig.getInt(THRIFT_FRAME_SIZE_MB, THRIFT_DEFAULT_FRAME_SIZE) * 1024 * 1024;
    }

    public abstract Partitioner getPartitioner() throws StorageException;

    @Override
    public StoreTransaction beginTransaction(ConsistencyLevel level) {
        return new CassandraTransaction(level, readConsistencyLevel, writeConsistencyLevel);
    }

    @Override
    public String toString() {
        return "[" + keySpaceName + "@" + super.toString() + "]";
    }

    @Override
    public StoreFeatures getFeatures() {
        if (features == null) {
            features = new StoreFeatures();
            features.supportsScan = true;
            features.supportsBatchMutation = true;
            features.supportsTransactions = false;
            features.supportsConsistentKeyOperations = true;
            features.supportsLocking = false;
            features.isDistributed = true;

            Partitioner partitioner;
            try {
                partitioner = getPartitioner();
            } catch (StorageException e) {
                throw new TitanException("Could not connect to Cassandra to read partitioner information. Please check the connection", e);
            }
            features.supportsScan = true;
            if (partitioner == Partitioner.RANDOM) {
                features.isKeyOrdered = false;
                features.hasLocalKeyPartition = false;
            } else if (partitioner == Partitioner.BYTEORDER) {
                features.isKeyOrdered = true;
                features.hasLocalKeyPartition = false;
            } else if (partitioner == Partitioner.LOCALBYTEORDER) {
                features.isKeyOrdered = true;
                features.hasLocalKeyPartition = true;
            } else throw new IllegalArgumentException("Unrecognized partitioner: " + partitioner);
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
     * @param cf
     *            the name of the column family for which to return compression
     *            options
     * @return map of compression option names to compression option values
     * @throws StorageException
     *             if reading from Cassandra fails
     */
    public abstract Map<String, String> getCompressionOptions(String cf) throws StorageException;
    
    public String getName() {
        return getClass().getSimpleName() + keySpaceName;
    }
}

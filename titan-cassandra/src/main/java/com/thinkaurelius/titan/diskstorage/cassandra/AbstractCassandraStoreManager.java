package com.thinkaurelius.titan.diskstorage.cassandra;

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
 * (c) Matthias Broecheler (me@matthiasb.com)
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
     * Next tqo options should be appropriately set when server-side Thrift counterparts were changed,
     * because otherwise client wouldn't be able to accept read/write frames from server as incorrectly sized.
     *
     * Note: both properties are sized in megabytes for user convenience (defaults are 15MB and 16MB by cassandra.yaml).
     */
    public static final String THRIFT_FRAME_SIZE_MB = "cassandra.thrift.frame_size_mb";
    public static final String THRIFT_MAX_MESSAGE_SIZE_MB = "cassandra.thrift.max_message_size_mb";

    public static final int THRIFT_DEFAULT_FRAME_SIZE = 15 * 1024 * 1024;
    public static final int THRIFT_DEFAULT_MAX_MESSAGE_SIZE = 16 * 1024 * 1024;

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
    protected final int thriftFrameSize, thriftMaxMessageSize;

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

        String rawFrameSize = storageConfig.getString(THRIFT_FRAME_SIZE_MB);
        String rawMaxMessageSize = storageConfig.getString(THRIFT_MAX_MESSAGE_SIZE_MB);

        try {
            this.thriftFrameSize = (rawFrameSize != null)
                                     ? Integer.valueOf(rawFrameSize) * 1024 * 1024
                                     : THRIFT_DEFAULT_FRAME_SIZE;

            this.thriftMaxMessageSize = (rawMaxMessageSize != null)
                                         ? Integer.valueOf(rawMaxMessageSize) * 1024 * 1024
                                         : THRIFT_DEFAULT_MAX_MESSAGE_SIZE;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Thrift storage option(s) given", e);
        }
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

            Partitioner partitioner = null;
            try {
                partitioner = getPartitioner();
            } catch (StorageException e) {
                throw new TitanException("Could not read partitioner information", e);
            }
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


}

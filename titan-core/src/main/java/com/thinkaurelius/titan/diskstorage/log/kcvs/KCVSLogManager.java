package com.thinkaurelius.titan.diskstorage.log.kcvs;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.ttl.TTLKVCSManager;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.configuration.PreInitializeConfigOptions;
import com.thinkaurelius.titan.graphdb.database.idassigner.placement.PartitionIDRange;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoInstanceCacheImpl;
import com.thinkaurelius.titan.util.encoding.ConversionHelper;
import com.thinkaurelius.titan.util.stats.NumberUtil;
import com.thinkaurelius.titan.util.system.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Implementation of {@link LogManager} against an arbitrary {@link KeyColumnValueStoreManager}. Issues {@link Log} instances
 * which wrap around a {@link KeyColumnValueStore}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@PreInitializeConfigOptions
public class KCVSLogManager implements LogManager {

    private static final Logger log =
            LoggerFactory.getLogger(KCVSLogManager.class);

    public static final ConfigOption<Boolean> LOG_FIXED_PARTITION = new ConfigOption<Boolean>(LOG_NS,"fixed-partition",
            "Whether all log entries are written to one fixed partition even if the backend store is partitioned." +
                    "This can cause imbalanced loads and should only be used on low volume logs",
            ConfigOption.Type.GLOBAL_OFFLINE, false);

    public static final ConfigOption<Integer> LOG_MAX_PARTITIONS = new ConfigOption<Integer>(LOG_NS,"max-partitions",
            "The maximum number of partitions to use for logging. Setting up this many actual or virtual partitions. Must be bigger than 1" +
                    "and a power of 2.",
            ConfigOption.Type.FIXED, Integer.class, new Predicate<Integer>() {
        @Override
        public boolean apply(@Nullable Integer integer) {
            return integer!=null && integer>1 && NumberUtil.isPowerOf2(integer);
        }
    });


    /**
     * Configuration of this log manager
     */
    private final Configuration configuration;
    /**
     * Store Manager against which to open the {@link KeyColumnValueStore}s to wrap the {@link KCVSLog} around.
     */
    final KeyColumnValueStoreManager storeManager;
    /**
     * Id which uniquely identifies this instance. Also see {@link GraphDatabaseConfiguration#UNIQUE_INSTANCE_ID}.
     */
    final String senderId;

    /**
     * The number of first bits of the key that identifies a partition. If this number is X then there are 2^X different
     * partition blocks each of which is identified by a partition id.
     */
    final int partitionBitWidth;
    /**
     * A collection of partition ids to which the logs write in round-robin fashion.
     */
    final int[] defaultWritePartitionIds;
    /**
     * A collection of partition ids from which the readers will read concurrently.
     */
    final int[] readPartitionIds;
    /**
     * Serializer used to (de)-serialize the log messages
     */
    final StandardSerializer serializer;

    /**
     * Keeps track of all open logs
     */
    private final Map<String,KCVSLog> openLogs;

    /**
     * Opens a log manager against the provided KCVS store with the given configuration.
     * @param storeManager
     * @param config
     */
    public KCVSLogManager(final KeyColumnValueStoreManager storeManager, final Configuration config) {
        this(storeManager, config, null);
    }

    /**
     * Opens a log manager against the provided KCVS store with the given configuration. Also provided is a list
     * of read-partition-ids. These only apply when readers are registered against an opened log. In that case,
     * the readers only read from the provided list of partition ids.
     * @param storeManager
     * @param config
     * @param readPartitionIds
     */
    public KCVSLogManager(KeyColumnValueStoreManager storeManager, final Configuration config,
                          final int[] readPartitionIds) {
        Preconditions.checkArgument(storeManager!=null && config!=null);
        if (config.has(LOG_STORE_TTL)) {
            if (TTLKVCSManager.supportsStoreTTL(storeManager)) {
                storeManager = new TTLKVCSManager(storeManager, ConversionHelper.getTTLSeconds(config.get(LOG_STORE_TTL)));
            } else {
                log.warn("Log is configured with TTL but underlying storage backend does not support TTL, hence this" +
                        "configuration option is ignored and entries must be manually removed from the backend.");
            }
        }
        this.storeManager = storeManager;
        this.configuration = config;
        openLogs = new HashMap<String, KCVSLog>();

        this.senderId=config.get(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID);
        Preconditions.checkNotNull(senderId);

        if (config.get(CLUSTER_PARTITION)) {
            ConfigOption<Integer> maxPartitionConfig = config.has(LOG_MAX_PARTITIONS)?
                                                        LOG_MAX_PARTITIONS:CLUSTER_MAX_PARTITIONS;
            int maxPartitions = config.get(maxPartitionConfig);
            Preconditions.checkArgument(maxPartitions<=config.get(CLUSTER_MAX_PARTITIONS),
                    "Number of log partitions cannot be larger than number of cluster partitions");
            this.partitionBitWidth= NumberUtil.getPowerOf2(maxPartitions);
        } else {
            this.partitionBitWidth=0;
        }
        Preconditions.checkArgument(partitionBitWidth>=0 && partitionBitWidth<32);
        final int numPartitions = (1<<partitionBitWidth);

        //Partitioning
        if (config.get(CLUSTER_PARTITION) && !config.get(LOG_FIXED_PARTITION)) {
            //Write partitions - default initialization: writing to all partitions
            int[] writePartitions = new int[numPartitions];
            for (int i=0;i<numPartitions;i++) writePartitions[i]=i;
            if (storeManager.getFeatures().hasLocalKeyPartition()) {
                //Write only to local partitions
                List<Integer> localPartitions = new ArrayList<Integer>();
                try {
                    List<PartitionIDRange> partitionRanges = PartitionIDRange.getIDRanges(partitionBitWidth,
                            storeManager.getLocalKeyPartition());
                    for (PartitionIDRange idrange : partitionRanges) {
                        for (int p : idrange.getAllContainedIDs()) localPartitions.add(p);
                    }
                } catch (Throwable e) {
                    log.error("Could not process local id partitions",e);
                }

                if (!localPartitions.isEmpty()) {
                    writePartitions = new int[localPartitions.size()];
                    for (int i=0;i<localPartitions.size();i++) writePartitions[i]=localPartitions.get(i);
                }
            }
            this.defaultWritePartitionIds = writePartitions;
            //Read partitions
            if (readPartitionIds!=null && readPartitionIds.length>0) {
                for (int readPartitionId : readPartitionIds) {
                    checkValidPartitionId(readPartitionId,partitionBitWidth);
                }
                this.readPartitionIds = Arrays.copyOf(readPartitionIds,readPartitionIds.length);
            } else {
                this.readPartitionIds=new int[numPartitions];
                for (int i=0;i<numPartitions;i++) this.readPartitionIds[i]=i;
            }
        } else {
            this.defaultWritePartitionIds=new int[]{0};
            Preconditions.checkArgument(readPartitionIds==null || (readPartitionIds.length==0 && readPartitionIds[0]==0),
                    "Cannot configure read partition ids on unpartitioned backend or with fixed partitions enabled");
            this.readPartitionIds=new int[]{0};
        }

        KryoInstanceCacheImpl kcache = configuration.get(GraphDatabaseConfiguration.KRYO_INSTANCE_CACHE);
        this.serializer = new StandardSerializer(false, kcache);
    }

    private static void checkValidPartitionId(int partitionId, int partitionBitWidth) {
        Preconditions.checkArgument(partitionId>=0 && partitionId<(1<<partitionBitWidth));
    }

    @Override
    public synchronized KCVSLog openLog(final String name) throws BackendException {
        if (openLogs.containsKey(name)) return openLogs.get(name);
        KCVSLog log = new KCVSLog(name,this,storeManager.openDatabase(name),configuration);
        openLogs.put(name,log);
        return log;
    }

    /**
     * Must be triggered by a particular {@link KCVSLog} when it is closed so that this log can be removed from the list
     * of open logs.
     * @param log
     */
    synchronized void closedLog(KCVSLog log) {
        KCVSLog l = openLogs.remove(log.getName());
        assert l==log;
    }

    @Override
    public synchronized void close() throws BackendException {
        /* Copying the map is necessary to avoid ConcurrentModificationException.
         * The path to ConcurrentModificationException in the absence of a copy is
         * log.close() -> manager.closedLog(log) -> openLogs.remove(log.getName()).
         */
        for (KCVSLog log : ImmutableMap.copyOf(openLogs).values()) log.close();

        IOUtils.closeQuietly(serializer);
    }

}

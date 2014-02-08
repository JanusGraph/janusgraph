package com.thinkaurelius.titan.diskstorage.log.kcvs;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.diskstorage.log.ReadMarker;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.graphdb.database.serialize.Serializer;
import com.thinkaurelius.titan.graphdb.database.serialize.StandardSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link LogManager} against an arbitrary {@link KeyColumnValueStoreManager}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSLogManager implements LogManager {

    private final Map<String,KCVSLog> openLogs;
    private final Configuration configuration;
    final KeyColumnValueStoreManager storeManager;

    final int partitionBitWidth;
    final int defaultPartitionId;
    final int[] readPartitionIds;
    final String senderId;
    final Serializer serializer;

    public KCVSLogManager(final KeyColumnValueStoreManager storeManager, final String senderId, final Configuration config) {
        this(storeManager, senderId, 0, 0, new int[]{0},config);
    }

    public KCVSLogManager(final KeyColumnValueStoreManager storeManager, final String senderId,
                          final int partitionBitWidth, final int defaultPartitionId, final int[] readPartitionIds, final Configuration config) {
        Preconditions.checkArgument(storeManager!=null && config!=null);
        this.storeManager = storeManager;
        this.configuration = config;
        openLogs = new HashMap<String, KCVSLog>();

        Preconditions.checkNotNull(senderId);
        this.senderId=senderId;
        Preconditions.checkArgument(partitionBitWidth>=0 && partitionBitWidth<32);
        this.partitionBitWidth=partitionBitWidth;
        this.defaultPartitionId=defaultPartitionId;
        checkValidPartitionId(defaultPartitionId,partitionBitWidth);
        Preconditions.checkArgument(readPartitionIds.length>0);
        for (int i = 0; i < readPartitionIds.length; i++) {
            checkValidPartitionId(readPartitionIds[i],partitionBitWidth);
        }
        this.readPartitionIds = Arrays.copyOf(readPartitionIds,readPartitionIds.length);

        this.serializer = new StandardSerializer(false);
    }

    private static void checkValidPartitionId(int partitionId, int partitionBitWidth) {
        Preconditions.checkArgument(partitionId>=0 && partitionId<(1<<partitionBitWidth));
    }

    @Override
    public synchronized Log openLog(final String name, ReadMarker readMarker) throws StorageException {
        if (openLogs.containsKey(name)) return openLogs.get(name);
        KCVSLog log = new KCVSLog(name,this,storeManager.openDatabase(name),readMarker,configuration);
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
    public synchronized void close() throws StorageException {
        for (KCVSLog log : openLogs.values()) log.close();
    }

}

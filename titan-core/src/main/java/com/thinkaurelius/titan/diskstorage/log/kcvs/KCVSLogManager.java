package com.thinkaurelius.titan.diskstorage.log.kcvs;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.log.Log;
import com.thinkaurelius.titan.diskstorage.log.LogManager;
import com.thinkaurelius.titan.diskstorage.log.ReadMarker;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class KCVSLogManager implements LogManager {

    private final Map<String,KCVSLog> openLogs;
    private final boolean hashKey=false;
    private final Configuration configuration;

    final int partitionBitWidth=0;
    final int defaultPartitionId=0;
    final String senderId=null;
    final KeyColumnValueStoreManager storeManager=null;

    public KCVSLogManager(Configuration config) {
        Preconditions.checkNotNull(config);
        this.configuration = config;
        openLogs = new HashMap<String, KCVSLog>();

        Preconditions.checkNotNull(senderId);
        Preconditions.checkArgument(partitionBitWidth>=0 && partitionBitWidth<32);
        Preconditions.checkArgument(defaultPartitionId >= 0 && defaultPartitionId<(1<<partitionBitWidth)-1);

    }

    @Override
    public synchronized Log openLog(final String name, ReadMarker readMarker) throws StorageException {
        if (openLogs.containsKey(name)) return openLogs.get(name);
        KCVSLog log = new KCVSLog(name,this,storeManager.openDatabase(name),readMarker,configuration);
        openLogs.put(name,log);
        return log;
    }

    synchronized void closedLog(KCVSLog log) {
        KCVSLog l = openLogs.remove(log.getName());
        assert l==log;
    }

    @Override
    public synchronized void close() throws StorageException {
        for (KCVSLog log : openLogs.values()) log.close();
    }

    StaticBuffer hashKey(StaticBuffer key) {
        if (!hashKey) return key;
        throw new UnsupportedOperationException(); //TODO: hash!
    }


}

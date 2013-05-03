package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConsistentKeyLockConfiguration {

    final byte[] rid;
    final int lockRetryCount;
    final long lockExpireMS;
    final long lockWaitMS;
    final String localLockMediatorPrefix;

    public ConsistentKeyLockConfiguration(Configuration config, String storeManagerName) {
        this.rid = DistributedStoreManager.getRid(config);

        this.localLockMediatorPrefix = config.getString(
                ConsistentKeyLockStore.LOCAL_LOCK_MEDIATOR_PREFIX_KEY,
                storeManagerName);

        this.lockRetryCount = config.getInt(
                GraphDatabaseConfiguration.LOCK_RETRY_COUNT,
                GraphDatabaseConfiguration.LOCK_RETRY_COUNT_DEFAULT);

        this.lockWaitMS = config.getLong(
                GraphDatabaseConfiguration.LOCK_WAIT_MS,
                GraphDatabaseConfiguration.LOCK_WAIT_MS_DEFAULT);

        this.lockExpireMS = config.getLong(
                GraphDatabaseConfiguration.LOCK_EXPIRE_MS,
                GraphDatabaseConfiguration.LOCK_EXPIRE_MS_DEFAULT);
    }

}

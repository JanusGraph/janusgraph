package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.configuration.Configuration;

/**
 * A class that holds configuration specific to interprocess locking.
 * 
 * @see ConsistentKeyLockTransaction
 * @see ConsistentKeyLockStore
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConsistentKeyLockConfiguration {

    /**
     * "Requestor ID": each process within a Titan installation must have a
     * unique rid. The value itself does not matter so long as each process has
     * a unique one.
     */
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

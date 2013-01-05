package com.thinkaurelius.titan.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.commons.configuration.Configuration;

import java.nio.ByteBuffer;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractIDManager implements IDAuthority {

    /* This value can't be changed without either
      * corrupting existing ID allocations or taking
      * some additional action to prevent such
      * corruption.
      */
    protected static final long BASE_ID = 1;

    protected final long idApplicationWaitMS;
    protected final int idApplicationRetryCount;

    protected final byte[] rid;

    private IDBlockSizer blockSizer;
    private volatile boolean isActive;

    public AbstractIDManager(Configuration config) {
        this.rid = DistributedStoreManager.getRid(config);

        this.isActive = false;

        this.idApplicationWaitMS =
                config.getLong(
                        GraphDatabaseConfiguration.IDAUTHORITY_WAIT_MS_KEY,
                        GraphDatabaseConfiguration.IDAUTHORITY_WAIT_MS_DEFAULT);

        this.idApplicationRetryCount =
                config.getInt(
                        GraphDatabaseConfiguration.IDAUTHORITY_RETRY_COUNT_KEY,
                        GraphDatabaseConfiguration.IDAUTHORITY_RETRY_COUNT_DEFAULT);
    }

    @Override
    public synchronized void setIDBlockSizer(IDBlockSizer sizer) {
        Preconditions.checkNotNull(sizer);
        if (isActive) throw new IllegalStateException("IDBlockSizer cannot be changed after IDAuthority is in use");
        this.blockSizer = sizer;
    }

    protected ByteBuffer getPartitionKey(int partition) {
        return ByteBufferUtil.getIntByteBuffer(partition);
    }

    protected long getBlockSize(int partition) {
        Preconditions.checkArgument(blockSizer != null, "Blocksizer has not yet been initialized");
        isActive = true;
        return blockSizer.getBlockSize(partition);
    }

}

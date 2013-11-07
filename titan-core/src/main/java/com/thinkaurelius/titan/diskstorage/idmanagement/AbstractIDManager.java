package com.thinkaurelius.titan.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.IDAuthority;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.common.DistributedStoreManager;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.graphdb.database.idassigner.IDBlockSizer;
import org.apache.commons.configuration.Configuration;

/**
 * Base Class for {@link IDAuthority} implementations.
 * Handles common aspects such as maintaining the {@link IDBlockSizer} and shared configuration options
 *
 * @author Matthias Broecheler (me@matthiasb.com)
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
    
    protected final String metricsPrefix;

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

        this.metricsPrefix = GraphDatabaseConfiguration.getSystemMetricsPrefix();
    }

    @Override
    public synchronized void setIDBlockSizer(IDBlockSizer sizer) {
        Preconditions.checkNotNull(sizer);
        if (isActive) throw new IllegalStateException("IDBlockSizer cannot be changed after IDAuthority is in use");
        this.blockSizer = sizer;
    }

    /**
     * Returns a byte buffer representation for the given partition id
     * @param partition
     * @return
     */
    protected StaticBuffer getPartitionKey(int partition) {
        return ByteBufferUtil.getIntBuffer(partition);
    }

    /**
     * Returns the block size of the specified partition as determined by the configured {@link IDBlockSizer}.
     * @param partition
     * @return
     */
    protected long getBlockSize(final int partition) {
        Preconditions.checkArgument(blockSizer != null, "Blocksizer has not yet been initialized");
        isActive = true;
        long blockSize = blockSizer.getBlockSize(partition);
        Preconditions.checkArgument(blockSize>0,"Invalid block size: %s",blockSize);
        Preconditions.checkArgument(blockSize<getIdUpperBound(partition),
                "Block size [%s] cannot be larger than upper bound [%s] for partition [%s]",blockSize,getIdUpperBound(partition),partition);
        return blockSize;
    }

    protected long getIdUpperBound(final int partition) {
        Preconditions.checkArgument(blockSizer != null, "Blocksizer has not yet been initialized");
        isActive = true;
        long upperBound = blockSizer.getIdUpperBound(partition);
        Preconditions.checkArgument(upperBound>0,"Invalid upper bound: %s",upperBound);
        return upperBound;
    }

}

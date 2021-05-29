// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.idmanagement;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.IDAuthority;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.database.idassigner.IDBlockSizer;

import java.time.Duration;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID;
import static org.janusgraph.util.encoding.StringEncoding.UTF8_CHARSET;


/**
 * Base Class for {@link IDAuthority} implementations.
 * Handles common aspects such as maintaining the {@link IDBlockSizer} and shared configuration options
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class AbstractIDAuthority implements IDAuthority {

    /* This value can't be changed without either
      * corrupting existing ID allocations or taking
      * some additional action to prevent such
      * corruption.
      */
    protected static final long BASE_ID = 1;

    protected final Duration idApplicationWaitMS;

    protected final String uid;
    protected final byte[] uidBytes;

    protected final String metricsPrefix;

    private IDBlockSizer blockSizer;
    private volatile boolean isActive;

    public AbstractIDAuthority(Configuration config) {
        this.uid = config.get(UNIQUE_INSTANCE_ID);

        this.uidBytes = uid.getBytes(UTF8_CHARSET);

        this.isActive = false;

        this.idApplicationWaitMS =
                config.get(GraphDatabaseConfiguration.IDAUTHORITY_WAIT);

        this.metricsPrefix = GraphDatabaseConfiguration.METRICS_SYSTEM_PREFIX_DEFAULT;
    }

    @Override
    public synchronized void setIDBlockSizer(IDBlockSizer sizer) {
        Preconditions.checkNotNull(sizer);
        if (isActive) throw new IllegalStateException("IDBlockSizer cannot be changed after IDAuthority is in use");
        this.blockSizer = sizer;
    }

    @Override
    public String getUniqueID() {
        return uid;
    }

    /**
     * Returns a byte buffer representation for the given partition id
     * @param partition
     * @return
     */
    protected StaticBuffer getPartitionKey(int partition) {
        return BufferUtil.getIntBuffer(partition);
    }

    /**
     * Returns the block size of the specified partition as determined by the configured {@link IDBlockSizer}.
     * @param idNamespace
     * @return
     */
    protected long getBlockSize(final int idNamespace) {
        Preconditions.checkArgument(blockSizer != null, "Blocksizer has not yet been initialized");
        isActive = true;
        long blockSize = blockSizer.getBlockSize(idNamespace);
        Preconditions.checkArgument(blockSize>0,"Invalid block size: %s",blockSize);
        Preconditions.checkArgument(blockSize<getIdUpperBound(idNamespace),
                "Block size [%s] cannot be larger than upper bound [%s] for partition [%s]",blockSize,getIdUpperBound(idNamespace),idNamespace);
        return blockSize;
    }

    protected long getIdUpperBound(final int idNamespace) {
        Preconditions.checkArgument(blockSizer != null, "Blocksizer has not yet been initialized");
        isActive = true;
        long upperBound = blockSizer.getIdUpperBound(idNamespace);
        Preconditions.checkArgument(upperBound>0,"Invalid upper bound: %s",upperBound);
        return upperBound;
    }

}

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

package org.janusgraph.diskstorage.common;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.time.TimestampProvider;

import java.time.Duration;
import java.time.Instant;
import java.util.Random;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.CONNECTION_TIMEOUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.PAGE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_PORT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TIMESTAMP_PROVIDER;

/**
 * Abstract class that handles configuration options shared by all distributed storage backends
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class DistributedStoreManager extends AbstractStoreManager {

    protected final TimestampProvider times;

    public enum Deployment {

        /**
         * Connects to storage backend over the network or some other connection with significant latency
         */
        REMOTE,

        /**
         * Connects to storage backend over localhost or some other connection with very low latency
         */
        LOCAL,

        /**
         * Embedded with storage backend and communicates inside the JVM
         */
        EMBEDDED

    }


    private static final Random random = new Random();

    protected final String[] hostnames;
    protected final int port;
    protected final Duration connectionTimeoutMS;
    protected final int pageSize;

    protected final String username;
    protected final String password;



    public DistributedStoreManager(Configuration storageConfig, int portDefault) {
        super(storageConfig);
        this.hostnames = storageConfig.get(STORAGE_HOSTS);
        Preconditions.checkArgument(hostnames.length > 0, "No hostname configured");
        if (storageConfig.has(STORAGE_PORT)) this.port = storageConfig.get(STORAGE_PORT);
        else this.port = portDefault;
        this.connectionTimeoutMS = storageConfig.get(CONNECTION_TIMEOUT);
        this.pageSize = storageConfig.get(PAGE_SIZE);
        this.times = storageConfig.get(TIMESTAMP_PROVIDER);

        if (storageConfig.has(AUTH_USERNAME)) {
            this.username = storageConfig.get(AUTH_USERNAME);
            this.password = storageConfig.get(AUTH_PASSWORD);
        } else {
            this.username=null;
            this.password=null;
        }
    }

    /**
     * Returns a randomly chosen host name. This is used to pick one host when multiple are configured
     *
     * @return
     */
    protected String getSingleHostname() {
        return hostnames[random.nextInt(hostnames.length)];
    }

    /**
     * Whether authentication is enabled for this storage backend
     *
     * @return
     */
    public boolean hasAuthentication() {
        return username!=null;
    }

    /**
     * Returns the default configured page size for this storage backend. The page size is used to determine
     * the number of records to request at a time when streaming result data.
     * @return
     */
    public int getPageSize() {
        return pageSize;
    }

    /*
     * TODO this should go away once we have a JanusGraphConfig that encapsulates TimestampProvider
     */
    public TimestampProvider getTimestampProvider() {
        return times;
    }

    /**
     * Returns the {@link Deployment} mode of this connection to the storage backend
     *
     * @return
     */
    public abstract Deployment getDeployment();

    @Override
    public String toString() {
        String hn = getSingleHostname();
        return hn.substring(0, Math.min(hn.length(), 256)) + ":" + port;
    }

    protected void sleepAfterWrite(MaskedTimestamp mustPass) throws BackendException {
        assert mustPass.getDeletionTime(times) < mustPass.getAdditionTime(times);
        try {
            times.sleepPast(mustPass.getAdditionTimeInstant(times));
        } catch (InterruptedException e) {
            throw new PermanentBackendException("Unexpected interrupt", e);
        }
    }

    /**
     * Helper class to create the deletion and addition timestamps for a particular transaction.
     * It needs to be ensured that the deletion time is prior to the addition time since
     * some storage backends use the time to resolve conflicts.
     */
    public static class MaskedTimestamp {

        private final Instant t;

        public MaskedTimestamp(Instant commitTime) {
            Preconditions.checkNotNull(commitTime);
            this.t=commitTime;
        }

        public MaskedTimestamp(StoreTransaction txh) {
            this(txh.getConfiguration().getCommitTime());
        }

        public long getDeletionTime(TimestampProvider times) {
            return times.getTime(t)   & 0xFFFFFFFFFFFFFFFEL; // zero the LSB
        }

        public long getAdditionTime(TimestampProvider times) {
            return (times.getTime(t)   & 0xFFFFFFFFFFFFFFFEL) | 1L; // force the LSB to 1
        }

        public Instant getAdditionTimeInstant(TimestampProvider times) {
            return times.getTime(getAdditionTime(times));
        }
    }
}

package com.thinkaurelius.titan.diskstorage.common;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimepoint;
import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.util.time.TimestampProvider;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Abstract class that handles configuration options shared by all distributed storage backends
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class DistributedStoreManager extends AbstractStoreManager {

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


    private static final Logger log = LoggerFactory.getLogger(DistributedStoreManager.class);
    private static final Random random = new Random();

    protected final String[] hostnames;
    protected final int port;
    protected final Duration connectionTimeoutMS;
    protected final int connectionPoolSize;
    protected final int pageSize;

    protected final String username;
    protected final String password;

    protected final TimestampProvider times; // TODO remove

    public DistributedStoreManager(Configuration storageConfig, int portDefault) {
        super(storageConfig);
        this.hostnames = storageConfig.get(STORAGE_HOSTS);
        Preconditions.checkArgument(hostnames.length > 0, "No hostname configured");
        if (storageConfig.has(STORAGE_PORT)) this.port = storageConfig.get(STORAGE_PORT);
        else this.port = portDefault;
        this.connectionTimeoutMS = storageConfig.get(CONNECTION_TIMEOUT);
        this.connectionPoolSize = storageConfig.get(CONNECTION_POOL_SIZE);
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
     * TODO this should go away once we have a TitanConfig that encapsulates TimestampProvider
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

    /**
     * This method attempts to generate Rid in the following three ways, in order,
     * returning the value produced by the first successful attempt in the sequence.
     * <p/>
     * <ol>
     * <li>
     * If {@code config} contains {@see GraphDatabaseConfiguration#INSTANCE_RID_RAW_KEY},
     * then read it as a String value.  Convert the String returned into a char[] and
     * call {@code org.apache.commons.codec.binary.Hex#decodeHex on the char[]}.  The
     * byte[] returned by {@code decodeHex} is then returned as Rid.
     * </li>
     * <li>
     * If {@code config} contains {@see GraphDatabaseConfiguration#INSTANCE_RID_SHORT_KEY},
     * then read it as a short value.  Call {@see java.net.InetAddress#getLocalHost()},
     * and on its return value call {@see java.net.InetAddress#getAddress()} to retrieve
     * the machine's IP address in byte[] form.  The returned Rid is a byte[] containing
     * the localhost address bytes in its lower indices and the short value in its
     * penultimate and final indices.
     * </li>
     * <li>
     * If both of the previous failed, then call
     * {@see java.lang.management.RuntimeMXBean#getName()} and then call
     * {@code String#getBytes()} on the returned value.  Return a Rid as described in the
     * previous point, replacing the short value with the byte[] representing the JVM name.
     * </li>
     * </ol>
     *
     * @param config commons config from which to read Rid-related keys
     * @return A byte array which should uniquely identify this machine
     */
//    public static byte[] getRid(Configuration config) {
//        Preconditions.checkArgument(config.has(UNIQUE_INSTANCE_ID));
//        return config.get(UNIQUE_INSTANCE_ID).getBytes();
//
//        byte tentativeRid[] = null;
//
//        if (config.has(GraphDatabaseConfiguration.INSTANCE_RID_RAW)) {
//            String ridText =
//                    config.get(GraphDatabaseConfiguration.INSTANCE_RID_RAW);
//            try {
//                tentativeRid = Hex.decodeHex(ridText.toCharArray());
//            } catch (DecoderException e) {
//                throw new TitanConfigurationException("Could not decode hex value", e);
//            }
//
//            log.debug("Set rid from hex string: 0x{}", ridText);
//        } else {
//            final byte[] endBytes;
//
//            if (config.has(GraphDatabaseConfiguration.INSTANCE_RID_SHORT)) {
//
//                short s = config.get(
//                        GraphDatabaseConfiguration.INSTANCE_RID_SHORT);
//
//                endBytes = new byte[2];
//
//                endBytes[0] = (byte) ((s & 0x0000FF00) >> 8);
//                endBytes[1] = (byte) (s & 0x000000FF);
//            } else {
//                //endBytes = ManagementFactory.getRuntimeMXBean().getName().getBytes();
//                endBytes = new StringBuilder(String.valueOf(Thread.currentThread().getId()))
//                            .append("@")
//                            .append(ManagementFactory.getRuntimeMXBean().getName())
//                            .toString()
//                            .getBytes();
//            }
//
//            byte[] addrBytes;
//            try {
//                addrBytes = Inet4Address.getLocalHost().getAddress();
//            } catch (UnknownHostException e) {
//                throw new TitanConfigurationException("Unknown host specified", e);
//            }
//
//            tentativeRid = new byte[addrBytes.length + endBytes.length];
//            System.arraycopy(addrBytes, 0, tentativeRid, 0, addrBytes.length);
//            System.arraycopy(endBytes, 0, tentativeRid, addrBytes.length, endBytes.length);
//
//            if (log.isDebugEnabled()) {
//                log.debug("Set rid: 0x{}", new String(Hex.encodeHex(tentativeRid)));
//            }
//        }
//
//        return tentativeRid;
//    }

    protected void sleepAfterWrite(StoreTransaction txh, MaskedTimestamp mustPass) throws StorageException {
        assert mustPass.getDeletionTime(times.getUnit()) < mustPass.getAdditionTime(times.getUnit());
        try {
            times.sleepPast(mustPass.getAdditionTime());
        } catch (InterruptedException e) {
            throw new PermanentStorageException("Unexpected interrupt", e);
        }
    }

    /**
     * Helper class to create the deletion and addition timestamps for a particular transaction.
     * It needs to be ensured that the deletion time is prior to the addition time since
     * some storage backends use the time to resolve conflicts.
     */
    public class MaskedTimestamp {

        private final Timepoint t;

        public MaskedTimestamp(Timepoint commitTime) {
            Preconditions.checkNotNull(commitTime);
            this.t=commitTime;
        }

        public MaskedTimestamp(StoreTransaction txh) {
            this(txh.getConfiguration().getCommitTime());
        }

        public long getDeletionTime(TimeUnit unit) {
            return t.getTimestamp(unit)   & 0xFFFFFFFFFFFFFFFEL; // zero the LSB
        }

        public long getNativeDeletionTime() {
            assert t.getProvider().equals(times);
            return t.getNativeTimestamp() & 0xFFFFFFFFFFFFFFFEL; // zero the LSB
        }

        public Timepoint getDeletionTime() {
            return new StandardTimepoint(getDeletionTime(t.getNativeUnit()), t.getProvider());
        }

        public long getAdditionTime(TimeUnit unit) {
            return (t.getTimestamp(unit)   & 0xFFFFFFFFFFFFFFFEL) | 1L; // force the LSB to 1
        }

        public long getNativeAdditionTime() {
            assert t.getProvider().equals(times);
            return (t.getNativeTimestamp() & 0xFFFFFFFFFFFFFFFEL) | 1L; // force the LSB to 1
        }

        public Timepoint getAdditionTime() {
            return new StandardTimepoint(getAdditionTime(t.getNativeUnit()), t.getProvider());
        }
    }
}

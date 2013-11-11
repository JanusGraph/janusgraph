package com.thinkaurelius.titan.diskstorage.common;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanConfigurationException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.Random;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.*;

/**
 * Abstract class that handles configuration options shared by all distributed storage backends
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class DistributedStoreManager extends AbstractStoreManager {

    public enum Deployment {

        /**
         * Connects to storage backend over the network
         */
        REMOTE,

        /**
         * Connects to storage backend over localhost
         */
        LOCAL,

        /**
         * Embedded with storage backend
         */
        EMBEDDED;

    }


    private static final Logger log = LoggerFactory.getLogger(DistributedStoreManager.class);
    private static final Random random = new Random();

    protected final byte[] rid;

    protected final String[] hostnames;
    protected final int port;
    protected final int connectionTimeout;
    protected final int connectionPoolSize;
    protected final int pageSize;

    protected final String username;
    protected final String password;

    public DistributedStoreManager(Configuration storageConfig, int portDefault) {
        super(storageConfig);
        if (storageConfig.containsKey(HOSTNAME_KEY)) {
            this.hostnames = storageConfig.getStringArray(HOSTNAME_KEY);
        } else {
            this.hostnames = new String[]{HOSTNAME_DEFAULT};
        }
        Preconditions.checkArgument(hostnames.length > 0, "No hostname configured");
        this.port = storageConfig.getInt(GraphDatabaseConfiguration.PORT_KEY, portDefault);
        this.rid = getRid(storageConfig);
        this.connectionTimeout = storageConfig.getInt(CONNECTION_TIMEOUT_KEY, CONNECTION_TIMEOUT_DEFAULT);
        this.connectionPoolSize = storageConfig.getInt(CONNECTION_POOL_SIZE_KEY, CONNECTION_POOL_SIZE_DEFAULT);
        this.pageSize = storageConfig.getInt(PAGE_SIZE_KEY, PAGE_SIZE_DEFAULT);


        this.username = storageConfig.getString(GraphDatabaseConfiguration.AUTH_USERNAME_KEY,null);
        this.password = storageConfig.getString(GraphDatabaseConfiguration.AUTH_PASSWORD_KEY,null);
    }

    /**
     * Returns a randomly chosen host name. This is used to pick one host when multiple are configured
     *
     * @return
     */
    protected String getSingleHostname() {
        return hostnames[random.nextInt(hostnames.length)];
    }

    public boolean hasAuthentication() {
        return username!=null;
    }

    public int getPageSize() {
        return pageSize;
    }

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
    public static byte[] getRid(Configuration config) {

        byte tentativeRid[] = null;

        if (config.containsKey(GraphDatabaseConfiguration.INSTANCE_RID_RAW_KEY)) {
            String ridText =
                    config.getString(GraphDatabaseConfiguration.INSTANCE_RID_RAW_KEY);
            try {
                tentativeRid = Hex.decodeHex(ridText.toCharArray());
            } catch (DecoderException e) {
                throw new TitanConfigurationException("Could not decode hex value", e);
            }

            log.debug("Set rid from hex string: 0x{}", ridText);
        } else {
            final byte[] endBytes;

            if (config.containsKey(GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY)) {

                short s = config.getShort(
                        GraphDatabaseConfiguration.INSTANCE_RID_SHORT_KEY);

                endBytes = new byte[2];

                endBytes[0] = (byte) ((s & 0x0000FF00) >> 8);
                endBytes[1] = (byte) (s & 0x000000FF);
            } else {
                //endBytes = ManagementFactory.getRuntimeMXBean().getName().getBytes();
                endBytes = new StringBuilder(String.valueOf(Thread.currentThread().getId()))
                            .append("@")
                            .append(ManagementFactory.getRuntimeMXBean().getName())
                            .toString()
                            .getBytes();
            }

            byte[] addrBytes;
            try {
                addrBytes = Inet4Address.getLocalHost().getAddress();
            } catch (UnknownHostException e) {
                throw new TitanConfigurationException("Unknown host specified", e);
            }

            tentativeRid = new byte[addrBytes.length + endBytes.length];
            System.arraycopy(addrBytes, 0, tentativeRid, 0, addrBytes.length);
            System.arraycopy(endBytes, 0, tentativeRid, addrBytes.length, endBytes.length);

            if (log.isDebugEnabled()) {
                log.debug("Set rid: 0x{}", new String(Hex.encodeHex(tentativeRid)));
            }
        }

        return tentativeRid;
    }

    protected Timestamp getTimestamp(StoreTransaction txh) {
        long time = txh.getConfiguration().getTimestamp();
        time = time & 0xFFFFFFFFFFFFFFFEL; //remove last bit
        return new Timestamp(time | 1L, time);
    }

    public static class Timestamp {
        public final long additionTime;
        public final long deletionTime;

        public Timestamp(long additionTime, long deletionTime) {
            Preconditions.checkArgument(0 < deletionTime, "Negative time: %s", deletionTime);
            Preconditions.checkArgument(deletionTime < additionTime, "%s vs %s", deletionTime, additionTime);
            this.additionTime = additionTime;
            this.deletionTime = deletionTime;
        }
    }

}

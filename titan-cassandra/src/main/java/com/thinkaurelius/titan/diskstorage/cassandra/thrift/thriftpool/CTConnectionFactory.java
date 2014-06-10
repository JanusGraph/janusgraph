package com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.diskstorage.PermanentStorageException;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.TemporaryStorageException;
import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A factory compatible with Apache commons-pool for Cassandra Thrift API
 * connections.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CTConnectionFactory implements KeyedPoolableObjectFactory<String, CTConnection> {

    private static final Logger log = LoggerFactory.getLogger(CTConnectionFactory.class);
    private static final long SCHEMA_WAIT_MAX = 5000L;
    private static final long SCHEMA_WAIT_INCREMENT = 25L;

    private final AtomicReference<Config> cfgRef;

    private CTConnectionFactory(Config config) {
        this.cfgRef = new AtomicReference<Config>(config);
    }

    @Override
    public void activateObject(String key, CTConnection c) throws Exception {
        // Do nothing, as in passivateObject
    }

    @Override
    public void destroyObject(String key, CTConnection c) throws Exception {
        TTransport t = c.getTransport();

        if (t.isOpen())
            t.close();
    }

    @Override
    public CTConnection makeObject(String key) throws Exception {
        CTConnection conn = makeRawConnection();
        Cassandra.Client client = conn.getClient();
        client.set_keyspace(key);

        return conn;
    }

    /**
     * Create a Cassandra-Thrift connection, but do not attempt to
     * set a keyspace on the connection.
     *
     * @return A CTConnection ready to talk to a Cassandra cluster
     * @throws TTransportException on any Thrift transport failure
     */
    public CTConnection makeRawConnection() throws TTransportException {
        final Config cfg = cfgRef.get();

        String hostname = cfg.getRandomHost();

        if (log.isDebugEnabled())
            log.debug("Creating TSocket({}, {}, {}, {}, {})", hostname, cfg.port, cfg.username, cfg.password, cfg.timeoutMS);


        TSocket socket;
        if (null != cfg.sslTruststoreLocation && !cfg.sslTruststoreLocation.isEmpty()) {
            TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters() {{
               setKeyStore(cfg.sslTruststoreLocation, cfg.sslTruststorePassword);
            }};
            socket = TSSLTransportFactory.getClientSocket(hostname, cfg.port, cfg.timeoutMS, params);
        } else {
            socket = new TSocket(hostname, cfg.port, cfg.timeoutMS);
        }

        TTransport transport = new TFramedTransport(socket, cfg.frameSize);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        transport.open();

        if (cfg.username != null) {
            Map<String, String> credentials = new HashMap<String, String>() {{
                put(IAuthenticator.USERNAME_KEY, cfg.username);
                put(IAuthenticator.PASSWORD_KEY, cfg.password);
            }};

            try {
                client.login(new AuthenticationRequest(credentials));
            } catch (Exception e) { // TTransportException will propagate authentication/authorization failure
                throw new TTransportException(e);
            }
        }
        return new CTConnection(transport, client, cfg);
    }

    @Override
    public void passivateObject(String key, CTConnection o) throws Exception {
        // Do nothing, as in activateObject
    }

    @Override
    public boolean validateObject(String key, CTConnection c) {
        Config curCfg = cfgRef.get();

        boolean isSameConfig = c.getConfig().equals(curCfg);
        if (log.isDebugEnabled()) {
            if (isSameConfig) {
                log.debug("Validated Thrift connection {}", c);
            } else {
                log.debug("Rejected Thrift connection {}; current config is {}; connection config is {}",
                          c, curCfg, c.getConfig());
            }
        }

        return isSameConfig && c.isOpen();
    }

    public static class Config {
        // this is to keep backward compatibility with JDK 1.6, can be changed to ThreadLocalRandom once we fully switch
        private static final ThreadLocal<Random> THREAD_LOCAL_RANDOM = new ThreadLocal<Random>() {
            @Override
            public Random initialValue() {
                return new Random();
            }
        };

        private final String[] hostnames;
        private final int port;
        private final String username;
        private final String password;

        private int timeoutMS;
        private int frameSize;

        private String sslTruststoreLocation;
        private String sslTruststorePassword;

        private boolean isBuilt;

        public Config(String[] hostnames, int port, String username, String password) {
            this.hostnames = hostnames;
            this.port = port;
            this.username = username;
            this.password = password;
        }

        // TODO: we don't really need getters/setters here as all of the fields are final and immutable

        public String getHostname() {
            return hostnames[0];
        }

        public int getPort() {
            return port;
        }

        public String getRandomHost() {
            return hostnames.length == 1 ? hostnames[0] : hostnames[THREAD_LOCAL_RANDOM.get().nextInt(hostnames.length)];
        }

        public Config setTimeoutMS(int timeoutMS) {
            checkIfAlreadyBuilt();
            this.timeoutMS = timeoutMS;
            return this;
        }

        public Config setFrameSize(int frameSize) {
            checkIfAlreadyBuilt();
            this.frameSize = frameSize;
            return this;
        }

        public Config setSSLTruststoreLocation(String location) {
            checkIfAlreadyBuilt();
            this.sslTruststoreLocation = location;
            return this;
        }

        public Config setSSLTruststorePassword(String password) {
            checkIfAlreadyBuilt();
            this.sslTruststorePassword = password;
            return this;
        }

        public CTConnectionFactory build() {
            isBuilt = true;
            return new CTConnectionFactory(this);
        }


        public void checkIfAlreadyBuilt() {
            if (isBuilt)
                throw new IllegalStateException("Can't accept modifications when used with built factory.");
        }

        @Override
        public String toString() {
            return "Config[hostnames=" + StringUtils.join(hostnames, ',') + ", port=" + port
                    + ", timeoutMS=" + timeoutMS + ", frameSize=" + frameSize
                    + "]";
        }
    }

}


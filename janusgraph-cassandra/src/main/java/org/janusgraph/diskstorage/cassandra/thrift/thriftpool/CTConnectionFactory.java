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

package org.janusgraph.diskstorage.cassandra.thrift.thriftpool;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.auth.IAuthenticator;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        if (t.isOpen()) {
            t.close();
            log.trace("Closed transport {}", t);
        } else {
            log.trace("Not closing transport {} (already closed)", t);
        }
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

        log.debug("Creating TSocket({}, {}, {}, {}, {})", hostname, cfg.port, cfg.username, cfg.password, cfg.timeoutMS);

        TSocket socket;
        if (null != cfg.sslTruststoreLocation && !cfg.sslTruststoreLocation.isEmpty()) {
            TSSLTransportFactory.TSSLTransportParameters params = new TSSLTransportFactory.TSSLTransportParameters() {{
               setTrustStore(cfg.sslTruststoreLocation, cfg.sslTruststorePassword);
            }};
            socket = TSSLTransportFactory.getClientSocket(hostname, cfg.port, cfg.timeoutMS, params);
        } else {
            socket = new TSocket(hostname, cfg.port, cfg.timeoutMS);
        }

        TTransport transport = new TFramedTransport(socket, cfg.frameSize);
        log.trace("Created transport {}", transport);
        TBinaryProtocol protocol = new TBinaryProtocol(transport);
        Cassandra.Client client = new Cassandra.Client(protocol);
        if (!transport.isOpen()) {
            transport.open();
        }

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
                log.trace("Validated {} by configuration {}", c, curCfg);
            } else {
                log.trace("Rejected {}; current config is {}; rejected connection config is {}",
                          c, curCfg, c.getConfig());
            }
        }
        
        boolean isOpen = c.isOpen();
        try {
            c.getClient().describe_version();
        } catch (TException e) {
            isOpen = false;
        }

        return isSameConfig && isOpen;
    }

    public static class Config {

        private final String[] hostnames;
        private final int port;
        private final String username;
        private final String password;
        private final Random random;

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
            this.random = new Random();
        }

        // TODO: we don't really need getters/setters here as all of the fields are final and immutable

        public String getHostname() {
            return hostnames[0];
        }

        public int getPort() {
            return port;
        }

        public String getRandomHost() {
            return hostnames.length == 1 ? hostnames[0] : hostnames[random.nextInt(hostnames.length)];
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


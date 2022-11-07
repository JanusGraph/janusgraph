package org.apache.cassandra.hadoop.cql3;
/*
*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*
*/

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.google.common.base.Optional;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;


public class CqlConfigHelper
{
    private static final String INPUT_CQL_COLUMNS_CONFIG = "cassandra.input.columnfamily.columns";
    private static final String INPUT_CQL_PAGE_ROW_SIZE_CONFIG = "cassandra.input.page.row.size";
    private static final String INPUT_CQL_WHERE_CLAUSE_CONFIG = "cassandra.input.where.clause";
    private static final String INPUT_CQL = "cassandra.input.cql";

    private static final String USERNAME = "cassandra.username";
    private static final String PASSWORD = "cassandra.password";

    private static final String INPUT_NATIVE_PORT = "cassandra.input.native.port";
    private static final String INPUT_NATIVE_CORE_CONNECTIONS_PER_HOST = "cassandra.input.native.core.connections.per.host";
    private static final String INPUT_NATIVE_MAX_CONNECTIONS_PER_HOST = "cassandra.input.native.max.connections.per.host";
    private static final String INPUT_NATIVE_MAX_SIMULT_REQ_PER_CONNECTION = "cassandra.input.native.max.simult.reqs.per.connection";
    private static final String INPUT_NATIVE_CONNECTION_TIMEOUT = "cassandra.input.native.connection.timeout";
    private static final String INPUT_NATIVE_READ_CONNECTION_TIMEOUT = "cassandra.input.native.read.connection.timeout";
    private static final String INPUT_NATIVE_RECEIVE_BUFFER_SIZE = "cassandra.input.native.receive.buffer.size";
    private static final String INPUT_NATIVE_SEND_BUFFER_SIZE = "cassandra.input.native.send.buffer.size";
    private static final String INPUT_NATIVE_SOLINGER = "cassandra.input.native.solinger";
    private static final String INPUT_NATIVE_TCP_NODELAY = "cassandra.input.native.tcp.nodelay";
    private static final String INPUT_NATIVE_REUSE_ADDRESS = "cassandra.input.native.reuse.address";
    private static final String INPUT_NATIVE_KEEP_ALIVE = "cassandra.input.native.keep.alive";
    private static final String INPUT_NATIVE_AUTH_PROVIDER = "cassandra.input.native.auth.provider";
    private static final String INPUT_NATIVE_SSL_TRUST_STORE_PATH = "cassandra.input.native.ssl.trust.store.path";
    private static final String INPUT_NATIVE_SSL_KEY_STORE_PATH = "cassandra.input.native.ssl.key.store.path";
    private static final String INPUT_NATIVE_SSL_TRUST_STORE_PASSWARD = "cassandra.input.native.ssl.trust.store.password";
    private static final String INPUT_NATIVE_SSL_KEY_STORE_PASSWARD = "cassandra.input.native.ssl.key.store.password";
    private static final String INPUT_NATIVE_SSL_CIPHER_SUITES = "cassandra.input.native.ssl.cipher.suites";

    private static final String INPUT_NATIVE_PROTOCOL_VERSION = "cassandra.input.native.protocol.version";

    public static void setUserNameAndPassword(Configuration conf, String username, String password)
    {
        if (StringUtils.isNotBlank(username))
        {
            conf.set(INPUT_NATIVE_AUTH_PROVIDER, PlainTextAuthProvider.class.getName());
            conf.set(USERNAME, username);
            conf.set(PASSWORD, password);
        }
    }

    public static Optional<Integer> getInputCoreConnections(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_CORE_CONNECTIONS_PER_HOST, conf);
    }

    public static Optional<Integer> getInputMaxConnections(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_MAX_CONNECTIONS_PER_HOST, conf);
    }

    public static int getInputNativePort(Configuration conf)
    {
        return Integer.parseInt(conf.get(INPUT_NATIVE_PORT, "9042"));
    }

    public static Optional<Integer> getInputMaxSimultReqPerConnections(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_MAX_SIMULT_REQ_PER_CONNECTION, conf);
    }

    public static Optional<Integer> getInputNativeConnectionTimeout(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_CONNECTION_TIMEOUT, conf);
    }

    public static Optional<Integer> getInputNativeReadConnectionTimeout(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_READ_CONNECTION_TIMEOUT, conf);
    }

    public static Optional<Integer> getInputNativeReceiveBufferSize(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_RECEIVE_BUFFER_SIZE, conf);
    }

    public static Optional<Integer> getInputNativeSendBufferSize(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_SEND_BUFFER_SIZE, conf);
    }

    public static Optional<Integer> getInputNativeSolinger(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_SOLINGER, conf);
    }

    public static Optional<Boolean> getInputNativeTcpNodelay(Configuration conf)
    {
        return getBooleanSetting(INPUT_NATIVE_TCP_NODELAY, conf);
    }

    public static Optional<Boolean> getInputNativeReuseAddress(Configuration conf)
    {
        return getBooleanSetting(INPUT_NATIVE_REUSE_ADDRESS, conf);
    }

    public static Optional<String> getInputNativeAuthProvider(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_AUTH_PROVIDER, conf);
    }

    public static Optional<String> getInputNativeSSLTruststorePath(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_TRUST_STORE_PATH, conf);
    }

    public static Optional<String> getInputNativeSSLKeystorePath(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_KEY_STORE_PATH, conf);
    }

    public static Optional<String> getInputNativeSSLKeystorePassword(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_KEY_STORE_PASSWARD, conf);
    }

    public static Optional<String> getInputNativeSSLTruststorePassword(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_TRUST_STORE_PASSWARD, conf);
    }

    public static Optional<String> getInputNativeSSLCipherSuites(Configuration conf)
    {
        return getStringSetting(INPUT_NATIVE_SSL_CIPHER_SUITES, conf);
    }

    public static Optional<Boolean> getInputNativeKeepAlive(Configuration conf)
    {
        return getBooleanSetting(INPUT_NATIVE_KEEP_ALIVE, conf);
    }

    public static String getInputcolumns(Configuration conf)
    {
        return conf.get(INPUT_CQL_COLUMNS_CONFIG);
    }

    public static Optional<Integer> getInputPageRowSize(Configuration conf)
    {
        return getIntSetting(INPUT_CQL_PAGE_ROW_SIZE_CONFIG, conf);
    }

    public static String getInputWhereClauses(Configuration conf)
    {
        return conf.get(INPUT_CQL_WHERE_CLAUSE_CONFIG);
    }

    public static String getInputCql(Configuration conf)
    {
        return conf.get(INPUT_CQL);
    }

    private static Optional<Integer> getProtocolVersion(Configuration conf)
    {
        return getIntSetting(INPUT_NATIVE_PROTOCOL_VERSION, conf);
    }

    public static Cluster getInputCluster(String[] hosts, Configuration conf)
    {
        int port = getInputNativePort(conf);
        return getCluster(hosts, conf, port);
    }

    public static Cluster getCluster(String[] hosts, Configuration conf, int port)
    {
        Optional<AuthProvider> authProvider = getAuthProvider(conf);
        Optional<SSLOptions> sslOptions = getSSLOptions(conf);
        Optional<Integer> protocolVersion = getProtocolVersion(conf);
        LoadBalancingPolicy loadBalancingPolicy = getReadLoadBalancingPolicy(hosts);
        SocketOptions socketOptions = getReadSocketOptions(conf);
        QueryOptions queryOptions = getReadQueryOptions(conf);
        PoolingOptions poolingOptions = getReadPoolingOptions(conf);

        Cluster.Builder builder = Cluster.builder()
                .addContactPoints(hosts)
                .withPort(port)
                .withCompression(ProtocolOptions.Compression.NONE);

        if (authProvider.isPresent())
            builder.withAuthProvider(authProvider.get());
        if (sslOptions.isPresent())
            builder.withSSL(sslOptions.get());

        if (protocolVersion.isPresent())
        {
            builder.withProtocolVersion(ProtocolVersion.fromInt(protocolVersion.get()));
        }
        builder.withLoadBalancingPolicy(loadBalancingPolicy)
                .withSocketOptions(socketOptions)
                .withQueryOptions(queryOptions)
                .withPoolingOptions(poolingOptions);

        return builder.build();
    }

    public static void setInputNativePort(Configuration conf, String port)
    {
        conf.set(INPUT_NATIVE_PORT, port);
    }

    private static PoolingOptions getReadPoolingOptions(Configuration conf)
    {
        Optional<Integer> coreConnections = getInputCoreConnections(conf);
        Optional<Integer> maxConnections = getInputMaxConnections(conf);
        Optional<Integer> maxSimultaneousRequests = getInputMaxSimultReqPerConnections(conf);

        PoolingOptions poolingOptions = new PoolingOptions();

        for (HostDistance hostDistance : Arrays.asList(HostDistance.LOCAL, HostDistance.REMOTE))
        {
            if (coreConnections.isPresent())
                poolingOptions.setCoreConnectionsPerHost(hostDistance, coreConnections.get());
            if (maxConnections.isPresent())
                poolingOptions.setMaxConnectionsPerHost(hostDistance, maxConnections.get());
            if (maxSimultaneousRequests.isPresent())
                poolingOptions.setNewConnectionThreshold(hostDistance, maxSimultaneousRequests.get());
        }

        return poolingOptions;
    }

    private static QueryOptions getReadQueryOptions(Configuration conf)
    {
        String CL = ConfigHelper.getReadConsistencyLevel(conf);
        Optional<Integer> fetchSize = getInputPageRowSize(conf);
        QueryOptions queryOptions = new QueryOptions();
        if (CL != null && !CL.isEmpty())
            queryOptions.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.valueOf(CL));

        if (fetchSize.isPresent())
            queryOptions.setFetchSize(fetchSize.get());
        return queryOptions;
    }

    private static SocketOptions getReadSocketOptions(Configuration conf)
    {
        SocketOptions socketOptions = new SocketOptions();
        Optional<Integer> connectTimeoutMillis = getInputNativeConnectionTimeout(conf);
        Optional<Integer> readTimeoutMillis = getInputNativeReadConnectionTimeout(conf);
        Optional<Integer> receiveBufferSize = getInputNativeReceiveBufferSize(conf);
        Optional<Integer> sendBufferSize = getInputNativeSendBufferSize(conf);
        Optional<Integer> soLinger = getInputNativeSolinger(conf);
        Optional<Boolean> tcpNoDelay = getInputNativeTcpNodelay(conf);
        Optional<Boolean> reuseAddress = getInputNativeReuseAddress(conf);
        Optional<Boolean> keepAlive = getInputNativeKeepAlive(conf);

        if (connectTimeoutMillis.isPresent())
            socketOptions.setConnectTimeoutMillis(connectTimeoutMillis.get());
        if (readTimeoutMillis.isPresent())
            socketOptions.setReadTimeoutMillis(readTimeoutMillis.get());
        if (receiveBufferSize.isPresent())
            socketOptions.setReceiveBufferSize(receiveBufferSize.get());
        if (sendBufferSize.isPresent())
            socketOptions.setSendBufferSize(sendBufferSize.get());
        if (soLinger.isPresent())
            socketOptions.setSoLinger(soLinger.get());
        if (tcpNoDelay.isPresent())
            socketOptions.setTcpNoDelay(tcpNoDelay.get());
        if (reuseAddress.isPresent())
            socketOptions.setReuseAddress(reuseAddress.get());
        if (keepAlive.isPresent())
            socketOptions.setKeepAlive(keepAlive.get());

        return socketOptions;
    }

    private static LoadBalancingPolicy getReadLoadBalancingPolicy(final String[] stickHosts)
    {
        return new LimitedLocalNodeFirstLocalBalancingPolicy(stickHosts);
    }

    private static Optional<AuthProvider> getDefaultAuthProvider(Configuration conf)
    {
        Optional<String> username = getStringSetting(USERNAME, conf);
        Optional<String> password = getStringSetting(PASSWORD, conf);

        if (username.isPresent() && password.isPresent())
        {
            return Optional.of(new PlainTextAuthProvider(username.get(), password.get()));
        }
        else
        {
            return Optional.absent();
        }
    }

    private static Optional<AuthProvider> getAuthProvider(Configuration conf)
    {
        Optional<String> authProvider = getInputNativeAuthProvider(conf);
        if (!authProvider.isPresent())
            return getDefaultAuthProvider(conf);

        return Optional.of(getClientAuthProvider(authProvider.get(), conf));
    }

    public static Optional<SSLOptions> getSSLOptions(Configuration conf)
    {
        Optional<String> truststorePath = getInputNativeSSLTruststorePath(conf);

        if (truststorePath.isPresent())
        {
            Optional<String> keystorePath = getInputNativeSSLKeystorePath(conf);
            Optional<String> truststorePassword = getInputNativeSSLTruststorePassword(conf);
            Optional<String> keystorePassword = getInputNativeSSLKeystorePassword(conf);
            Optional<String> cipherSuites = getInputNativeSSLCipherSuites(conf);

            SSLContext context;
            try
            {
                context = getSSLContext(truststorePath, truststorePassword, keystorePath, keystorePassword);
            }
            catch (UnrecoverableKeyException | KeyManagementException |
                    NoSuchAlgorithmException | KeyStoreException | CertificateException | IOException e)
            {
                throw new RuntimeException(e);
            }
            String[] css = null;
            if (cipherSuites.isPresent())
                css = cipherSuites.get().split(",");

            return Optional.of(RemoteEndpointAwareJdkSSLOptions.builder()
                                            .withSSLContext(context)
                                            .withCipherSuites(css)
                                            .build());
        }
        return Optional.absent();
    }

    private static Optional<Integer> getIntSetting(String parameter, Configuration conf)
    {
        String setting = conf.get(parameter);
        if (setting == null)
            return Optional.absent();
        return Optional.of(Integer.valueOf(setting));
    }

    private static Optional<Boolean> getBooleanSetting(String parameter, Configuration conf)
    {
        String setting = conf.get(parameter);
        if (setting == null)
            return Optional.absent();
        return Optional.of(Boolean.valueOf(setting));
    }

    private static Optional<String> getStringSetting(String parameter, Configuration conf)
    {
        String setting = conf.get(parameter);
        if (setting == null)
            return Optional.absent();
        return Optional.of(setting);
    }

    private static AuthProvider getClientAuthProvider(String factoryClassName, Configuration conf)
    {
        try
        {
            Class<?> c = Class.forName(factoryClassName);
            if (PlainTextAuthProvider.class.equals(c))
            {
                String username = getStringSetting(USERNAME, conf).or("");
                String password = getStringSetting(PASSWORD, conf).or("");
                return (AuthProvider) c.getConstructor(String.class, String.class)
                        .newInstance(username, password);
            }
            else
            {
                return (AuthProvider) c.newInstance();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException("Failed to instantiate auth provider:" + factoryClassName, e);
        }
    }

    private static SSLContext getSSLContext(Optional<String> truststorePath,
                                            Optional<String> truststorePassword,
                                            Optional<String> keystorePath,
                                            Optional<String> keystorePassword)
    throws NoSuchAlgorithmException,
           KeyStoreException,
           CertificateException,
           IOException,
           UnrecoverableKeyException,
           KeyManagementException
    {
        SSLContext ctx = SSLContext.getInstance("SSL");

        TrustManagerFactory tmf = null;
        if (truststorePath.isPresent())
        {
            try (InputStream tsf = Files.newInputStream(Paths.get(truststorePath.get())))
            {
                KeyStore ts = KeyStore.getInstance("JKS");
                ts.load(tsf, truststorePassword.isPresent() ? truststorePassword.get().toCharArray() : null);
                tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                tmf.init(ts);
            }
        }

        KeyManagerFactory kmf = null;
        if (keystorePath.isPresent())
        {
            try (InputStream ksf = Files.newInputStream(Paths.get(keystorePath.get())))
            {
                KeyStore ks = KeyStore.getInstance("JKS");
                ks.load(ksf, keystorePassword.isPresent() ? keystorePassword.get().toCharArray() : null);
                kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                kmf.init(ks, keystorePassword.isPresent() ? keystorePassword.get().toCharArray() : null);
            }
        }

        ctx.init(kmf != null ? kmf.getKeyManagers() : null,
                 tmf != null ? tmf.getTrustManagers() : null,
                 new SecureRandom());
        return ctx;
    }
}

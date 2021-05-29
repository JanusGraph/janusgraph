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

package org.janusgraph.diskstorage.es.rest;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.configuration.backend.CommonsConfiguration;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.es.rest.util.HttpAuthTypes;
import org.janusgraph.diskstorage.es.rest.util.RestClientAuthenticator;
import org.janusgraph.diskstorage.es.rest.util.SSLConfigurationCallback;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.util.system.ConfigurationUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.same;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RestClientSetupTest {

    private static final String INDEX_NAME = "junit";
    private static final String SCHEME_HTTP = "http";
    private static final String SCHEME_HTTPS = "https";

    private static final String ES_HOST_01 = "es-host-01";
    private static final String ES_HOST_02 = "es-host-02";

    private static final int ES_PORT = 8080;
    private static final int ES_SCROLL_KA =
            ElasticSearchIndex.ES_SCROLL_KEEP_ALIVE.getDefaultValue() * 2;
    private static final String ES_BULK_REFRESH =
            String.valueOf(!Boolean.valueOf(ElasticSearchIndex.BULK_REFRESH.getDefaultValue()));

    private static final Integer RETRY_ON_CONFLICT = ElasticSearchIndex.RETRY_ON_CONFLICT.getDefaultValue();

    private static final AtomicInteger instanceCount = new AtomicInteger();

    @Captor
    ArgumentCaptor<HttpHost[]> hostListCaptor;

    @Captor
    ArgumentCaptor<Integer> scrollKACaptor;

    @Spy
    private RestClientSetup restClientSetup = new RestClientSetup();

    @Mock
    private RestClient restClientMock;

    @Mock
    private RestElasticSearchClient restElasticSearchClientMock;

    @Mock
    private SSLContext sslContextMock;

    @Mock
    private RestClientBuilder restClientBuilderMock;

    @BeforeEach
    public void setUp() {
        doReturn(restClientMock).when(restClientBuilderMock).build();
    }

    private ElasticSearchClient baseConfigTest(Map<String, String> extraConfigValues) throws Exception {
        final CommonsConfiguration cc = new CommonsConfiguration(ConfigurationUtil.createBaseConfiguration());
        cc.set("index." + INDEX_NAME + ".backend", "elasticsearch");
        cc.set("index." + INDEX_NAME + ".elasticsearch.interface", "REST_CLIENT");
        for(Map.Entry<String, String> me: extraConfigValues.entrySet()) {
            cc.set(me.getKey(), me.getValue());
        }

        final ModifiableConfiguration config = new ModifiableConfiguration(GraphDatabaseConfiguration.ROOT_NS, cc,
                BasicConfiguration.Restriction.NONE);

        doReturn(restClientBuilderMock).
            when(restClientSetup).getRestClientBuilder(any());
        doReturn(restElasticSearchClientMock).when(restClientSetup).
            getElasticSearchClient(any(RestClient.class), anyInt(), anyBoolean());

        return restClientSetup.connect(config.restrictTo(INDEX_NAME));
    }

    private List<HttpHost[]> baseHostsConfigTest(Map<String, String> extraConfigValues) throws Exception {
        final ElasticSearchClient elasticClient = baseConfigTest(ImmutableMap.<String, String>builder().
                putAll(extraConfigValues).
                build());

        assertSame(restElasticSearchClientMock, elasticClient);

        verify(restClientSetup).getRestClient(hostListCaptor.capture(), any(Configuration.class));
        return hostListCaptor.getAllValues();
    }

    @Test
    public void testConnectBasicHttpConfigurationSingleHost() throws Exception {

        final List<HttpHost[]> hostsConfigured = baseHostsConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".hostname", ES_HOST_01).
                build());

        assertNotNull(hostsConfigured);
        assertEquals(1, hostsConfigured.size());

        final HttpHost host0 = hostsConfigured.get(0)[0];
        assertEquals(ES_HOST_01, host0.getHostName());
        assertEquals(SCHEME_HTTP, host0.getSchemeName());
        assertEquals(ElasticSearchIndex.HOST_PORT_DEFAULT, host0.getPort());

        verify(restClientSetup).getElasticSearchClient(same(restClientMock), scrollKACaptor.capture(), anyBoolean());
        assertEquals(ElasticSearchIndex.ES_SCROLL_KEEP_ALIVE.getDefaultValue().intValue(),
                scrollKACaptor.getValue().intValue());

        verify(restElasticSearchClientMock, never()).setBulkRefresh(anyString());
    }

    @Test
    public void testConnectBasicHttpConfigurationMultiHost() throws Exception {
        final List<HttpHost[]> hostsConfigured = baseHostsConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".hostname", ES_HOST_01 + "," + ES_HOST_02).
                build());

        assertNotNull(hostsConfigured);
        assertEquals(1, hostsConfigured.size());

        final HttpHost host0 = hostsConfigured.get(0)[0];
        assertEquals(ES_HOST_01, host0.getHostName());
        assertEquals(SCHEME_HTTP, host0.getSchemeName());
        assertEquals(ElasticSearchIndex.HOST_PORT_DEFAULT, host0.getPort());

        final HttpHost host1 = hostsConfigured.get(0)[1];
        assertEquals(ES_HOST_02, host1.getHostName());
        assertEquals(SCHEME_HTTP, host1.getSchemeName());
        assertEquals(ElasticSearchIndex.HOST_PORT_DEFAULT, host1.getPort());

        verify(restClientSetup).getElasticSearchClient(same(restClientMock), scrollKACaptor.capture(), anyBoolean());
        assertEquals(ElasticSearchIndex.ES_SCROLL_KEEP_ALIVE.getDefaultValue().intValue(),
                scrollKACaptor.getValue().intValue());

        verify(restElasticSearchClientMock, never()).setBulkRefresh(anyString());
    }

    @Test
    public void testConnectBasicHttpConfigurationAllOptions() throws Exception {

        final List<HttpHost[]> hostsConfigured = baseHostsConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".hostname", ES_HOST_01).
                put("index." + INDEX_NAME + ".port", String.valueOf(ES_PORT)).
                put("index." + INDEX_NAME + ".elasticsearch.scroll-keep-alive", String.valueOf(ES_SCROLL_KA)).
                put("index." + INDEX_NAME + ".elasticsearch.bulk-refresh", ES_BULK_REFRESH).
                put("index." + INDEX_NAME + ".elasticsearch.retry_on_conflict", String.valueOf(RETRY_ON_CONFLICT)).
                build());

        assertNotNull(hostsConfigured);
        assertEquals(1, hostsConfigured.size());

        HttpHost host0 = hostsConfigured.get(0)[0];
        assertEquals(ES_HOST_01, host0.getHostName());
        assertEquals(SCHEME_HTTP, host0.getSchemeName());
        assertEquals(ES_PORT, host0.getPort());

        verify(restClientSetup).getElasticSearchClient(same(restClientMock), scrollKACaptor.capture(), anyBoolean());
        assertEquals(ES_SCROLL_KA,
                scrollKACaptor.getValue().intValue());

        verify(restElasticSearchClientMock).setBulkRefresh(eq(ES_BULK_REFRESH));
        verify(restElasticSearchClientMock).setRetryOnConflict(eq(RETRY_ON_CONFLICT));

    }

    @Test
    public void testConnectBasicHttpsConfigurationSingleHost() throws Exception {

        final List<HttpHost[]> hostsConfigured = baseHostsConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".hostname", ES_HOST_01).
                put("index." + INDEX_NAME + ".elasticsearch.ssl.enabled", "true").
                build());

        assertNotNull(hostsConfigured);
        assertEquals(1, hostsConfigured.size());

        HttpHost host0 = hostsConfigured.get(0)[0];
        assertEquals(ES_HOST_01, host0.getHostName());
        assertEquals(SCHEME_HTTPS, host0.getSchemeName());
        assertEquals(ElasticSearchIndex.HOST_PORT_DEFAULT, host0.getPort());

        verify(restClientSetup).getElasticSearchClient(same(restClientMock), scrollKACaptor.capture(), anyBoolean());
        assertEquals(ElasticSearchIndex.ES_SCROLL_KEEP_ALIVE.getDefaultValue().intValue(),
                scrollKACaptor.getValue().intValue());

        verify(restElasticSearchClientMock, never()).setBulkRefresh(anyString());
        verify(restElasticSearchClientMock, times(1)).setRetryOnConflict(null);
    }

    private HttpClientConfigCallback authTestBase(Map<String, String> extraConfigValues) throws Exception {

        baseConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".backend", "elasticsearch").
                put("index." + INDEX_NAME + ".hostname", ES_HOST_01).
                putAll(extraConfigValues).
                build()
            );

        final ArgumentCaptor<HttpClientConfigCallback> hcccCaptor = ArgumentCaptor.forClass(HttpClientConfigCallback.class);

        // callback is passed to the client builder
        verify(restClientBuilderMock).setHttpClientConfigCallback(hcccCaptor.capture());

        final HttpClientConfigCallback hccc = hcccCaptor.getValue();
        assertNotNull(hccc);

        return hccc;
    }

    private RestClientBuilder.RequestConfigCallback configCallbackTestBase(Map<String, String> extraConfigValues) throws Exception {

        baseConfigTest(ImmutableMap.<String, String>builder().
            put("index." + INDEX_NAME + ".backend", "elasticsearch").
            put("index." + INDEX_NAME + ".hostname", ES_HOST_01).
            putAll(extraConfigValues).
            build()
        );

        final ArgumentCaptor<RestClientBuilder.RequestConfigCallback> rccCaptor =
            ArgumentCaptor.forClass(RestClientBuilder.RequestConfigCallback.class);

        // callback is passed to the client builder
        verify(restClientBuilderMock).setRequestConfigCallback(rccCaptor.capture());

        final RestClientBuilder.RequestConfigCallback rcc = rccCaptor.getValue();
        assertNotNull(rcc);

        return rcc;
    }

    private CredentialsProvider basicAuthTestBase(final Map<String, String> extraConfigValues, final String realm,
            final String username, final String password) throws Exception {
        final HttpClientConfigCallback hccc = authTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.interface", "REST_CLIENT").
                    put("index." + INDEX_NAME + ".elasticsearch.http.auth.type", HttpAuthTypes.BASIC.toString()).
                    put("index." + INDEX_NAME + ".elasticsearch.http.auth.basic.username", username).
                    put("index." + INDEX_NAME + ".elasticsearch.http.auth.basic.password", password).
                    put("index." + INDEX_NAME + ".elasticsearch.http.auth.basic.realm", realm).
                    putAll(extraConfigValues).
                    build()
                );

        final HttpAsyncClientBuilder hacb = mock(HttpAsyncClientBuilder.class);
        doReturn(hacb).when(hacb).setDefaultCredentialsProvider(anyObject());
        hccc.customizeHttpClient(hacb);

        final ArgumentCaptor<BasicCredentialsProvider> cpCaptor = ArgumentCaptor.forClass(BasicCredentialsProvider.class);
        verify(hacb).setDefaultCredentialsProvider(cpCaptor.capture());

        final CredentialsProvider cp = cpCaptor.getValue();
        assertNotNull(cp);

        return cp;
    }

    @Test
    public void testHttpBasicAuthConfiguration() throws Exception {

        // testing that the appropriate values are passed to the client builder via credentials provider
        final String testRealm = "testRealm";
        final String testUser = "testUser";
        final String testPassword = "testPassword";

        final CredentialsProvider cp = basicAuthTestBase(ImmutableMap.<String, String>builder().
                build(), testRealm, testUser, testPassword);

        final Credentials credentials = cp.getCredentials(new AuthScope(AuthScope.ANY_HOST, AuthScope.ANY_PORT, testRealm));
        assertNotNull(credentials);
        assertEquals(testUser, credentials.getUserPrincipal().getName());
        assertEquals(testPassword, credentials.getPassword());
    }

    @Test
    public void testConnectAndSocketTimeout() throws Exception {
        final RestClientBuilder.RequestConfigCallback rcc = configCallbackTestBase(
            ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".elasticsearch.connect-timeout", "5000").
                put("index." + INDEX_NAME + ".elasticsearch.socket-timeout", "60000").
                build()
        );

        // verifying that the custom callback is in the chain
        final RequestConfig.Builder rccb = mock(RequestConfig.Builder.class);
        when(rccb.setConnectTimeout(any(Integer.class))).thenReturn(rccb);
        ArgumentCaptor<Integer> connectArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> socketArgumentCaptor = ArgumentCaptor.forClass(Integer.class);
        rcc.customizeRequestConfig(rccb);


        verify(rccb).setConnectTimeout(connectArgumentCaptor.capture());
        verify(rccb).setSocketTimeout(socketArgumentCaptor.capture());


        assertEquals(5000, connectArgumentCaptor.getValue());
        assertEquals(60000, socketArgumentCaptor.getValue());
    }


    @Test
    public void testCustomAuthenticator() throws Exception {

        final String uniqueInstanceKey = String.valueOf(instanceCount.getAndIncrement());
        final String[] customAuthArgs = new String[]{
                uniqueInstanceKey,
                "arg1",
                "arg2"
        };
        final String serializedArgList = StringUtils.join(customAuthArgs,',');
        final HttpClientConfigCallback hccc = authTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.interface", "REST_CLIENT").
                    put("index." + INDEX_NAME + ".elasticsearch.http.auth.type", HttpAuthTypes.CUSTOM.toString()).
                    put("index." + INDEX_NAME + ".elasticsearch.http.auth.custom.authenticator-class", TestCustomAuthenticator.class.getName()).
                    put("index." + INDEX_NAME + ".elasticsearch.http.auth.custom.authenticator-args",
                            serializedArgList).
                    build()
                );

        verify(restClientSetup).getCustomAuthenticator(
                eq(TestCustomAuthenticator.class.getName()),
                eq(customAuthArgs));

        TestCustomAuthenticator customAuth = TestCustomAuthenticator.instanceMap.get(uniqueInstanceKey);
        assertNotNull(customAuth);

        // authenticator has been instantiated, verifying it has been called
        assertEquals(1, customAuth.numInitCalls);

        // verifying that the custom callback is in the chain
        final HttpAsyncClientBuilder hacb = mock(HttpAsyncClientBuilder.class);
        hccc.customizeHttpClient(hacb);

        assertEquals(1, customAuth.customizeHttpClientHistory.size());
        assertSame(hacb, customAuth.customizeHttpClientHistory.get(0));
        assertArrayEquals(customAuthArgs, customAuth.args);
    }

    public SSLConfigurationCallback.Builder sslSettingsTestBase(final Map<String, String> extraConfigValues) throws Exception {

        final SSLConfigurationCallback.Builder sslConfBuilderMock = mock(SSLConfigurationCallback.Builder.class);

        doReturn(sslConfBuilderMock).when(restClientSetup).getSSLConfigurationCallbackBuilder();

        authTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.interface", "REST_CLIENT").
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.enabled", "true").
                    putAll(extraConfigValues).
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        return sslConfBuilderMock;
    }

    @Test
    public void testSSLTrustStoreSettingsOnly() throws Exception {

        final String trustStoreFile = "/a/b/c/truststore.jks";
        final String trustStorePassword = "averysecretpassword";

        final SSLConfigurationCallback.Builder sslConfBuilderMock = sslSettingsTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.truststore.location", trustStoreFile).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.truststore.password", trustStorePassword).
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock).withTrustStore(eq(trustStoreFile), eq(trustStorePassword));
        verify(sslConfBuilderMock).build();
        verifyNoMoreInteractions(sslConfBuilderMock);
    }

    @Test
    public void testSSLKeyStoreSettingsOnly() throws Exception {

        final String keyStoreFile = "/a/b/c/keystore.jks";
        final String keyStorePassword = "key_store_password";
        final String keyPassword = "key_password";

        final SSLConfigurationCallback.Builder sslConfBuilderMock = sslSettingsTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.location", keyStoreFile).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.storepassword", keyStorePassword).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.keypassword", keyPassword).
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock).withKeyStore(eq(keyStoreFile), eq(keyStorePassword), eq(keyPassword));
        verify(sslConfBuilderMock).build();
        verifyNoMoreInteractions(sslConfBuilderMock);
    }

    @Test
    public void testSSLKeyStoreSettingsOnlyEmptyKeyPass() throws Exception {

        final String keyStoreFile = "/a/b/c/keystore.jks";
        final String keyStorePassword = "key_store_password";
        final String keyPassword = "";

        final SSLConfigurationCallback.Builder sslConfBuilderMock = sslSettingsTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.location", keyStoreFile).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.storepassword", keyStorePassword).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.keypassword", keyPassword).
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock).withKeyStore(eq(keyStoreFile), eq(keyStorePassword), eq(keyPassword));
        verify(sslConfBuilderMock).build();
        verifyNoMoreInteractions(sslConfBuilderMock);
    }

    @Test
    public void testSSLKeyStoreSettingsOnlyNoKeyPass() throws Exception {

        final String keyStoreFile = "/a/b/c/keystore.jks";
        final String keyStorePassword = "key_store_password";

        final SSLConfigurationCallback.Builder sslConfBuilderMock = sslSettingsTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.location", keyStoreFile).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.storepassword", keyStorePassword).
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock).withKeyStore(eq(keyStoreFile), eq(keyStorePassword), eq(keyStorePassword));
        verify(sslConfBuilderMock).build();
        verifyNoMoreInteractions(sslConfBuilderMock);
    }

    @Test
    public void testSSLKeyAndTrustStoreSettingsOnly() throws Exception {

        final String trustStoreFile = "/a/b/c/truststore.jks";
        final String trustStorePassword = "averysecretpassword";

        final String keyStoreFile = "/a/b/c/keystore.jks";
        final String keyStorePassword = "key_store_password";
        final String keyPassword = "key_password";

        final SSLConfigurationCallback.Builder sslConfBuilderMock = sslSettingsTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.truststore.location", trustStoreFile).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.truststore.password", trustStorePassword).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.location", keyStoreFile).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.storepassword", keyStorePassword).
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.keystore.keypassword", keyPassword).
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock).withTrustStore(eq(trustStoreFile), eq(trustStorePassword));
        verify(sslConfBuilderMock).withKeyStore(eq(keyStoreFile), eq(keyStorePassword), eq(keyPassword));
        verify(sslConfBuilderMock).build();
        verifyNoMoreInteractions(sslConfBuilderMock);
    }

    @Test
    public void testSSLDisableHostNameVerifier() throws Exception {

        final SSLConfigurationCallback.Builder sslConfBuilderMock = sslSettingsTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.disable-hostname-verification", "true").
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock).disableHostNameVerification();
        verify(sslConfBuilderMock).build();
        verifyNoMoreInteractions(sslConfBuilderMock);
    }

    @Test
    public void testSSLDisableHostNameVerifierExplicitOff() throws Exception {

        final SSLConfigurationCallback.Builder sslConfBuilderMock = mock(SSLConfigurationCallback.Builder.class);

        doReturn(sslConfBuilderMock).when(restClientSetup).getSSLConfigurationCallbackBuilder();

        baseConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".elasticsearch.ssl.enabled", "true").
                put("index." + INDEX_NAME + ".elasticsearch.ssl.disable-hostname-verification", "false").
                build()
            );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock, never()).disableHostNameVerification();
    }

    @Test
    public void testSSLDisableHostNameVerifierDefaultOff() throws Exception {

        final SSLConfigurationCallback.Builder sslConfBuilderMock = mock(SSLConfigurationCallback.Builder.class);

        doReturn(sslConfBuilderMock).when(restClientSetup).getSSLConfigurationCallbackBuilder();

        baseConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".elasticsearch.ssl.enabled", "true").
                build()
            );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock, never()).disableHostNameVerification();
    }

    @Test
    public void testSSLAllowSelfSignedCerts() throws Exception {

        final SSLConfigurationCallback.Builder sslConfBuilderMock = sslSettingsTestBase(
                ImmutableMap.<String, String>builder().
                    put("index." + INDEX_NAME + ".elasticsearch.ssl.allow-self-signed-certificates", "true").
                    build()
                );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock).allowSelfSignedCertificates();
        verify(sslConfBuilderMock).build();
        verifyNoMoreInteractions(sslConfBuilderMock);
    }

    @Test
    public void testSSLAllowSelfSignedCertsExplicitOff() throws Exception {

        final SSLConfigurationCallback.Builder sslConfBuilderMock = mock(SSLConfigurationCallback.Builder.class);

        doReturn(sslConfBuilderMock).when(restClientSetup).getSSLConfigurationCallbackBuilder();

        baseConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".elasticsearch.ssl.enabled", "true").
                put("index." + INDEX_NAME + ".elasticsearch.ssl.allow-self-signed-certificates", "false").
                build()
            );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock, never()).allowSelfSignedCertificates();
    }

    @Test
    public void testSSLAllowSelfSignedCertsDefaultOff() throws Exception {

        final SSLConfigurationCallback.Builder sslConfBuilderMock = mock(SSLConfigurationCallback.Builder.class);

        doReturn(sslConfBuilderMock).when(restClientSetup).getSSLConfigurationCallbackBuilder();

        baseConfigTest(ImmutableMap.<String, String>builder().
                put("index." + INDEX_NAME + ".elasticsearch.ssl.enabled", "true").
                build()
            );

        verify(restClientSetup).getSSLConfigurationCallbackBuilder();
        verify(sslConfBuilderMock, never()).allowSelfSignedCertificates();
    }

    public static class TestCustomAuthenticator implements RestClientAuthenticator {

        private static final Map<String, TestCustomAuthenticator> instanceMap = new HashMap<>();

        private final String[] args;

        private final List<Builder> customizeRequestConfigHistory = new LinkedList<>();
        private final List<HttpAsyncClientBuilder> customizeHttpClientHistory = new LinkedList<>();
        private int numInitCalls = 0;

        public TestCustomAuthenticator(String[] args) {
            this.args = args;
            Preconditions.checkArgument(instanceMap.put(args[0], this) == null, "Non-unique key used");
        }

        @Override
        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            customizeHttpClientHistory.add(httpClientBuilder);
            return httpClientBuilder;
        }

        @Override
        public Builder customizeRequestConfig(Builder requestConfigBuilder) {
            customizeRequestConfigHistory.add(requestConfigBuilder);
            return requestConfigBuilder;
        }

        @Override
        public void init() throws IOException {
            numInitCalls++;
        }
    }
}

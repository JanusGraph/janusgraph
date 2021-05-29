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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestClientBuilder.RequestConfigCallback;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.es.ElasticSearchClient;
import org.janusgraph.diskstorage.es.ElasticSearchIndex;
import org.janusgraph.diskstorage.es.rest.util.BasicAuthHttpClientConfigCallback;
import org.janusgraph.diskstorage.es.rest.util.ConnectionKeepAliveConfigCallback;
import org.janusgraph.diskstorage.es.rest.util.HttpAuthTypes;
import org.janusgraph.diskstorage.es.rest.util.RestClientAuthenticator;
import org.janusgraph.diskstorage.es.rest.util.SSLConfigurationCallback;
import org.janusgraph.diskstorage.es.rest.util.SSLConfigurationCallback.Builder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_HOSTS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.INDEX_PORT;

/**
 * Create an instance of Elasticsearch REST {@link org.elasticsearch.client.RestClient} from a JanusGraph
 * {@link org.janusgraph.diskstorage.configuration.Configuration}.
 */
public class RestClientSetup {

    private static final Logger log = LoggerFactory.getLogger(RestClientSetup.class);

    public ElasticSearchClient connect(Configuration config) throws IOException {
        log.debug("Configuring RestClient");

        final List<HttpHost> hosts = new ArrayList<>();
        final int defaultPort = config.has(INDEX_PORT) ? config.get(INDEX_PORT) : ElasticSearchIndex.HOST_PORT_DEFAULT;
        final String httpScheme = config.get(ElasticSearchIndex.SSL_ENABLED) ? "https" : "http";
        for (String host : config.get(INDEX_HOSTS)) {
            String[] hostStringParts = host.split(":");
            String hostname = hostStringParts[0];
            int hostPort = defaultPort;
            if (hostStringParts.length == 2) hostPort = Integer.parseInt(hostStringParts[1]);
            log.debug("Configured remote host: {} : {}", hostname, hostPort);
            hosts.add(new HttpHost(hostname, hostPort, httpScheme));
        }

        final RestClient rc = getRestClient(hosts.toArray(new HttpHost[hosts.size()]), config);

        final int scrollKeepAlive = config.get(ElasticSearchIndex.ES_SCROLL_KEEP_ALIVE);
        Preconditions.checkArgument(scrollKeepAlive >= 1, "Scroll keep-alive should be greater than or equal to 1");
        final boolean useMappingTypesForES7 = config.get(ElasticSearchIndex.USE_MAPPING_FOR_ES7);
        final RestElasticSearchClient client = getElasticSearchClient(rc, scrollKeepAlive, useMappingTypesForES7);
        if (config.has(ElasticSearchIndex.BULK_REFRESH)) {
            client.setBulkRefresh(config.get(ElasticSearchIndex.BULK_REFRESH));
        }

        Integer retryOnConflict = config.has(ElasticSearchIndex.RETRY_ON_CONFLICT) ? config.get(ElasticSearchIndex.RETRY_ON_CONFLICT) : null;
        client.setRetryOnConflict(retryOnConflict);

        return client;
    }

    protected RestClient getRestClient(HttpHost[] hosts, Configuration config) {
        final RestClientBuilder restClientBuilder = getRestClientBuilder(hosts);

        final HttpClientConfigCallback httpClientConfigCallback = getHttpClientConfigCallback(config);
        if (httpClientConfigCallback != null) {
            restClientBuilder.setHttpClientConfigCallback(httpClientConfigCallback);
        }

        final RequestConfigCallback requestConfigCallback = getRequestConfigCallback(config);
        if (requestConfigCallback != null) {
            restClientBuilder.setRequestConfigCallback(requestConfigCallback);
        }

        return restClientBuilder.build();
    }

    protected RestClientBuilder getRestClientBuilder(HttpHost[] hosts) {
        return RestClient.builder(hosts);
    }

    protected RestElasticSearchClient getElasticSearchClient(RestClient rc, int scrollKeepAlive, boolean useMappingTypesForES7) {
        return new RestElasticSearchClient(rc, scrollKeepAlive, useMappingTypesForES7);
    }

    /**
     * <p>
     * Returns the callback for customizing the {@link RequestConfig} or null if no
     * customization is needed.
     * </p>
     * <p>
     * See {@link RestClientBuilder#setRequestConfigCallback(RequestConfigCallback)} for more details.
     * </p>
     *
     * @param config
     *            ES index configuration
     * @return callback or null if the request customization is not needed
     */
    protected RequestConfigCallback getRequestConfigCallback(Configuration config) {

        final List<RequestConfigCallback> callbackList = new LinkedList<>();

        final Integer connectTimeout = config.get(ElasticSearchIndex.CONNECT_TIMEOUT);
        final Integer socketTimeout = config.get(ElasticSearchIndex.SOCKET_TIMEOUT);

        callbackList.add((requestConfigBuilder) ->
            requestConfigBuilder.setConnectTimeout(connectTimeout).setSocketTimeout(socketTimeout));

        // will execute the chain of individual callbacks
        return requestConfigBuilder -> {
            for(RequestConfigCallback cb: callbackList) {
                cb.customizeRequestConfig(requestConfigBuilder);
            }
            return requestConfigBuilder;
        };
    }

    /**
     * <p>
     * Returns the callback for customizing {@link CloseableHttpAsyncClient} or null if no
     * customization is needed.
     * </p>
     * <p>
     * See {@link RestClientBuilder#setHttpClientConfigCallback(HttpClientConfigCallback)} for more details.
     * </p>
     *
     * @param config
     *            ES index configuration
     * @return callback or null if the client customization is not needed
     */
    protected HttpClientConfigCallback getHttpClientConfigCallback(Configuration config) {

        final List<HttpClientConfigCallback> callbackList = new LinkedList<>();

        final HttpAuthTypes authType = ConfigOption.getEnumValue(config.get(ElasticSearchIndex.ES_HTTP_AUTH_TYPE),
                HttpAuthTypes.class);
        log.debug("Configuring HTTP(S) authentication type {}", authType);

        switch (authType) {
        case BASIC:
            callbackList.add(new BasicAuthHttpClientConfigCallback(
                    config.has(ElasticSearchIndex.ES_HTTP_AUTH_REALM) ? config.get(ElasticSearchIndex.ES_HTTP_AUTH_REALM) : "",
                    config.get(ElasticSearchIndex.ES_HTTP_AUTH_USERNAME),
                    config.get(ElasticSearchIndex.ES_HTTP_AUTH_PASSWORD)));
            break;
        case CUSTOM:
            callbackList.add(getCustomAuthenticator(
                    config.get(ElasticSearchIndex.ES_HTTP_AUTHENTICATOR_CLASS),
                    config.get(ElasticSearchIndex.ES_HTTP_AUTHENTICATOR_ARGS)));
            break;
        case NONE:
            break;
        default:
            // not expected
            throw new IllegalArgumentException("Authentication type \"" + authType + "\" is not implemented");
        }

        if (config.has(ElasticSearchIndex.CLIENT_KEEP_ALIVE )) {
            callbackList.add(new ConnectionKeepAliveConfigCallback(config.get(ElasticSearchIndex.CLIENT_KEEP_ALIVE)));
        }

        if (config.get(ElasticSearchIndex.SSL_ENABLED)) {
            // Custom SSL configuration
            final Builder sslConfCBBuilder = getSSLConfigurationCallbackBuilder();
            boolean configureSSL = false;
            if (config.has(ElasticSearchIndex.SSL_TRUSTSTORE_LOCATION)) {
                sslConfCBBuilder.withTrustStore(config.get(ElasticSearchIndex.SSL_TRUSTSTORE_LOCATION),
                        config.get(ElasticSearchIndex.SSL_TRUSTSTORE_PASSWORD));
                configureSSL = true;
            }
            if (config.has(ElasticSearchIndex.SSL_KEYSTORE_LOCATION)) {
                final String keystorePassword = config.get(ElasticSearchIndex.SSL_KEYSTORE_PASSWORD);
                sslConfCBBuilder.withKeyStore(config.get(ElasticSearchIndex.SSL_KEYSTORE_LOCATION),
                        keystorePassword,
                        config.has(ElasticSearchIndex.SSL_KEY_PASSWORD) ? config.get(ElasticSearchIndex.SSL_KEY_PASSWORD) : keystorePassword);
                configureSSL = true;
            }

            if (config.has(ElasticSearchIndex.SSL_DISABLE_HOSTNAME_VERIFICATION) &&
                config.get(ElasticSearchIndex.SSL_DISABLE_HOSTNAME_VERIFICATION)) {
                log.warn("SSL hostname verification is disabled, Elasticsearch HTTPS connections may not be secure");
                sslConfCBBuilder.disableHostNameVerification();
                configureSSL = true;
            }

            if (config.has(ElasticSearchIndex.SSL_ALLOW_SELF_SIGNED_CERTIFICATES) &&
                config.get(ElasticSearchIndex.SSL_ALLOW_SELF_SIGNED_CERTIFICATES)) {
                log.warn("Self-signed SSL certificate support is enabled, Elasticsearch HTTPS connections may not be secure");
                sslConfCBBuilder.allowSelfSignedCertificates();
                configureSSL = true;
            }

            if (configureSSL) {
                callbackList.add(sslConfCBBuilder.build());
            }
        }

        if (callbackList.isEmpty()) {
            return null;
        }

        // will execute the chain of individual callbacks
        return httpClientBuilder -> {
            for(HttpClientConfigCallback cb: callbackList) {
                cb.customizeHttpClient(httpClientBuilder);
            }
            return httpClientBuilder;
        };
    }

    protected SSLConfigurationCallback.Builder getSSLConfigurationCallbackBuilder() {
        return SSLConfigurationCallback.Builder.create();
    }

    protected RestClientAuthenticator getCustomAuthenticator(String authClassName, String[] authClassConstructorArgList) {
        Preconditions.checkArgument(StringUtils.isNotEmpty(authClassName),
                "Custom authenticator class name cannot be empty");
        Preconditions.checkNotNull(authClassConstructorArgList,
                "Custom authenticator class constructor argument list cannot be null");

        final RestClientAuthenticator authenticator;

        try {
            final Class<?> c = Class.forName(authClassName);
            Preconditions.checkArgument(RestClientAuthenticator.class.isAssignableFrom(c),
                "Authenticator class %s must be a subclass of %s", authClassName, RestClientAuthenticator.class.getName());
            @SuppressWarnings("unchecked")
            final Constructor<RestClientAuthenticator> ctr = ((Class<RestClientAuthenticator>)c).getConstructor(String[].class);
            authenticator = ctr.newInstance((Object)authClassConstructorArgList);
        } catch (Exception e) {
            log.error("Unable to instantiate the custom authenticator {} with constructor arguments \"{}\"",
                    authClassName, authClassConstructorArgList, e);
            throw new RuntimeException("Unable to instantiate the custom authenticator", e);
        }

        try {
            authenticator.init();
        } catch (IOException e) {
            log.error("Unable to initialize the custom authenticator {} with constructor arguments \"{}\"",
                    authClassName, authClassConstructorArgList, e);
            throw new RuntimeException("Unable to initialize the custom authenticator", e);
        }

        return authenticator;
    }
}

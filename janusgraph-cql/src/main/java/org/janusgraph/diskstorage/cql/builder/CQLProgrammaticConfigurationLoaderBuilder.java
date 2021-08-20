// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.cql.builder;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.auth.PlainTextAuthProvider;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.janusgraph.diskstorage.cql.CQLConfigOptions.HEARTBEAT_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.HEARTBEAT_TIMEOUT;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_DATACENTER;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.LOCAL_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.MAX_REQUESTS_PER_CONNECTION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_SCHEMA_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METADATA_TOKEN_MAP_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_EXPIRE_AFTER;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_MESSAGES_HIGHEST_LATENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_MESSAGES_REFRESH_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_NODE_MESSAGES_SIGNIFICANT_DIGITS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_REQUESTS_HIGHEST_LATENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_REQUESTS_REFRESH_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_REQUESTS_SIGNIFICANT_DIGITS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_THROTTLING_HIGHEST_LATENCY;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_THROTTLING_REFRESH_INTERVAL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.METRICS_SESSION_THROTTLING_SIGNIFICANT_DIGITS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_ADMIN_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_IO_SIZE;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_TIMER_TICKS_PER_WHEEL;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.NETTY_TIMER_TICK_DURATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.PROTOCOL_VERSION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REMOTE_MAX_CONNECTIONS_PER_HOST;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_ERROR_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_QUERY_LENGTH;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_VALUES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_MAX_VALUE_LENGTH;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SHOW_STACK_TRACES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SHOW_VALUES;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SLOW_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SLOW_THRESHOLD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_LOGGER_SUCCESS_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_TIMEOUT;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.REQUEST_TRACKER_CLASS;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SESSION_LEAK_THRESHOLD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SESSION_NAME;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_CLIENT_AUTHENTICATION_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_ENABLED;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_HOSTNAME_VALIDATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_KEYSTORE_KEY_PASSWORD;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_KEYSTORE_LOCATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_LOCATION;
import static org.janusgraph.diskstorage.cql.CQLConfigOptions.SSL_TRUSTSTORE_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_PASSWORD;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.AUTH_USERNAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.BASIC_METRICS;

public class CQLProgrammaticConfigurationLoaderBuilder {

    public DriverConfigLoader build(Configuration configuration, List<String> contactPoints, Duration connectionTimeoutMS) {

        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();

        configLoaderBuilder.withStringList(DefaultDriverOption.CONTACT_POINTS, contactPoints);

        configLoaderBuilder.withString(DefaultDriverOption.LOAD_BALANCING_LOCAL_DATACENTER, configuration.get(LOCAL_DATACENTER));

        configLoaderBuilder.withString(DefaultDriverOption.SESSION_NAME, configuration.get(SESSION_NAME));

        if(configuration.has(REQUEST_TIMEOUT)) {
            configLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(configuration.get(REQUEST_TIMEOUT)));
        }

        configLoaderBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, connectionTimeoutMS);

        if (configuration.get(PROTOCOL_VERSION) != 0) {
            configLoaderBuilder.withInt(DefaultDriverOption.PROTOCOL_VERSION, configuration.get(PROTOCOL_VERSION));
        }

        if (configuration.has(AUTH_USERNAME) && configuration.has(AUTH_PASSWORD)) {
            configLoaderBuilder
                .withClass(DefaultDriverOption.AUTH_PROVIDER_CLASS, PlainTextAuthProvider.class)
                .withString(DefaultDriverOption.AUTH_PROVIDER_USER_NAME, configuration.get(AUTH_USERNAME))
                .withString(DefaultDriverOption.AUTH_PROVIDER_PASSWORD, configuration.get(AUTH_PASSWORD));
        }

        if (configuration.get(SSL_ENABLED)) {
            configLoaderBuilder
                .withClass(DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS, DefaultSslEngineFactory.class)
                .withString(DefaultDriverOption.SSL_TRUSTSTORE_PATH, configuration.get(SSL_TRUSTSTORE_LOCATION))
                .withString(DefaultDriverOption.SSL_TRUSTSTORE_PASSWORD, configuration.get(SSL_TRUSTSTORE_PASSWORD))
                .withBoolean(DefaultDriverOption.SSL_HOSTNAME_VALIDATION, configuration.get(SSL_HOSTNAME_VALIDATION));

            if(configuration.get(SSL_CLIENT_AUTHENTICATION_ENABLED)) {
                configLoaderBuilder
                    .withString(DefaultDriverOption.SSL_KEYSTORE_PATH, configuration.get(SSL_KEYSTORE_LOCATION))
                    .withString(DefaultDriverOption.SSL_KEYSTORE_PASSWORD, configuration.get(SSL_KEYSTORE_KEY_PASSWORD));
            }
        }

        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_LOCAL_SIZE, configuration.get(LOCAL_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_POOL_REMOTE_SIZE, configuration.get(REMOTE_MAX_CONNECTIONS_PER_HOST));
        configLoaderBuilder.withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, configuration.get(MAX_REQUESTS_PER_CONNECTION));

        if(configuration.has(HEARTBEAT_INTERVAL)){
            configLoaderBuilder.withDuration(DefaultDriverOption.HEARTBEAT_INTERVAL,
                Duration.ofMillis(configuration.get(HEARTBEAT_INTERVAL)));
        }

        if(configuration.has(HEARTBEAT_TIMEOUT)){
            configLoaderBuilder.withDuration(DefaultDriverOption.HEARTBEAT_TIMEOUT,
                Duration.ofMillis(configuration.get(HEARTBEAT_TIMEOUT)));
        }

        if (configuration.has(METADATA_SCHEMA_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.METADATA_SCHEMA_ENABLED, configuration.get(METADATA_SCHEMA_ENABLED));
        }

        if (configuration.has(METADATA_TOKEN_MAP_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.METADATA_TOKEN_MAP_ENABLED, configuration.get(METADATA_TOKEN_MAP_ENABLED));
        }

        // Keep to 0 for the time being: https://groups.google.com/a/lists.datastax.com/forum/#!topic/java-driver-user/Bc0gQuOVVL0
        // Ideally we want to batch all tables initialisations to happen together when opening a new keyspace
        configLoaderBuilder.withInt(DefaultDriverOption.METADATA_SCHEMA_WINDOW, 0);

        configureCqlNetty(configuration, configLoaderBuilder);

        if (configuration.get(BASIC_METRICS)) {
            configureMetrics(configuration, configLoaderBuilder);
        }

        configureRequestTracker(configuration, configLoaderBuilder);

        if(configuration.has(SESSION_LEAK_THRESHOLD)){
            configLoaderBuilder.withInt(DefaultDriverOption.SESSION_LEAK_THRESHOLD, configuration.get(SESSION_LEAK_THRESHOLD));
        }

        return configLoaderBuilder.build();
    }

    private void configureCqlNetty(Configuration configuration, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder){
        // The following sets the size of Netty ThreadPool executor used by Cassandra driver:
        // https://docs.datastax.com/en/developer/java-driver/4.8/manual/core/async/#threading-model
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_IO_SIZE, configuration.get(NETTY_IO_SIZE));
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_ADMIN_SIZE, configuration.get(NETTY_ADMIN_SIZE));

        if(configuration.has(NETTY_TIMER_TICK_DURATION)){
            configLoaderBuilder.withDuration(DefaultDriverOption.NETTY_TIMER_TICK_DURATION,
                Duration.ofMillis(configuration.get(NETTY_TIMER_TICK_DURATION)));
        }
        if(configuration.has(NETTY_TIMER_TICKS_PER_WHEEL)){
            configLoaderBuilder.withInt(DefaultDriverOption.NETTY_TIMER_TICKS_PER_WHEEL, configuration.get(NETTY_TIMER_TICKS_PER_WHEEL));
        }

        // Keep the following values to 0 so that when we close the session we don't have to wait for the
        // so called "quiet period", setting this to a different value will slow down Graph.close()
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_IO_SHUTDOWN_QUIET_PERIOD, 0);
        configLoaderBuilder.withInt(DefaultDriverOption.NETTY_ADMIN_SHUTDOWN_QUIET_PERIOD, 0);
    }

    private void configureMetrics(Configuration configuration, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder){
        if(configuration.has(METRICS_SESSION_ENABLED)){
            configLoaderBuilder.withStringList(DefaultDriverOption.METRICS_SESSION_ENABLED,
                Arrays.asList(configuration.get(METRICS_SESSION_ENABLED)));
            if(configuration.has(METRICS_SESSION_REQUESTS_HIGHEST_LATENCY)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_HIGHEST,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_REQUESTS_HIGHEST_LATENCY)));
            }
            if(configuration.has(METRICS_SESSION_REQUESTS_SIGNIFICANT_DIGITS)){
                configLoaderBuilder.withInt(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_DIGITS,
                    configuration.get(METRICS_SESSION_REQUESTS_SIGNIFICANT_DIGITS));
            }
            if(configuration.has(METRICS_SESSION_REQUESTS_REFRESH_INTERVAL)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_CQL_REQUESTS_INTERVAL,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_REQUESTS_REFRESH_INTERVAL)));
            }
            if(configuration.has(METRICS_SESSION_THROTTLING_HIGHEST_LATENCY)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_THROTTLING_HIGHEST,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_THROTTLING_HIGHEST_LATENCY)));
            }
            if(configuration.has(METRICS_SESSION_THROTTLING_SIGNIFICANT_DIGITS)){
                configLoaderBuilder.withInt(DefaultDriverOption.METRICS_SESSION_THROTTLING_DIGITS,
                    configuration.get(METRICS_SESSION_THROTTLING_SIGNIFICANT_DIGITS));
            }
            if(configuration.has(METRICS_SESSION_THROTTLING_REFRESH_INTERVAL)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_SESSION_THROTTLING_INTERVAL,
                    Duration.ofMillis(configuration.get(METRICS_SESSION_THROTTLING_REFRESH_INTERVAL)));
            }
        }
        if(configuration.has(METRICS_NODE_ENABLED)){
            configLoaderBuilder.withStringList(DefaultDriverOption.METRICS_NODE_ENABLED,
                Arrays.asList(configuration.get(METRICS_NODE_ENABLED)));
            if(configuration.has(METRICS_NODE_MESSAGES_HIGHEST_LATENCY)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_HIGHEST,
                    Duration.ofMillis(configuration.get(METRICS_NODE_MESSAGES_HIGHEST_LATENCY)));
            }
            if(configuration.has(METRICS_NODE_MESSAGES_SIGNIFICANT_DIGITS)){
                configLoaderBuilder.withInt(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_DIGITS,
                    configuration.get(METRICS_NODE_MESSAGES_SIGNIFICANT_DIGITS));
            }
            if(configuration.has(METRICS_NODE_MESSAGES_REFRESH_INTERVAL)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_NODE_CQL_MESSAGES_INTERVAL,
                    Duration.ofMillis(configuration.get(METRICS_NODE_MESSAGES_REFRESH_INTERVAL)));
            }
            if(configuration.has(METRICS_NODE_EXPIRE_AFTER)){
                configLoaderBuilder.withDuration(DefaultDriverOption.METRICS_NODE_EXPIRE_AFTER,
                    Duration.ofMillis(configuration.get(METRICS_NODE_EXPIRE_AFTER)));
            }
        }
    }

    private void configureRequestTracker(Configuration configuration, ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder){
        if (configuration.has(REQUEST_TRACKER_CLASS)) {
            configLoaderBuilder.withString(DefaultDriverOption.REQUEST_TRACKER_CLASS, configuration.get(REQUEST_TRACKER_CLASS));
        }
        if (configuration.has(REQUEST_LOGGER_SUCCESS_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_SUCCESS_ENABLED,
                configuration.get(REQUEST_LOGGER_SUCCESS_ENABLED));
        }
        if (configuration.has(REQUEST_LOGGER_SLOW_THRESHOLD)) {
            configLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_LOGGER_SLOW_THRESHOLD,
                Duration.ofMillis(configuration.get(REQUEST_LOGGER_SLOW_THRESHOLD)));
        }
        if (configuration.has(REQUEST_LOGGER_SLOW_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_SLOW_ENABLED,
                configuration.get(REQUEST_LOGGER_SLOW_ENABLED));
        }
        if (configuration.has(REQUEST_LOGGER_ERROR_ENABLED)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_ERROR_ENABLED,
                configuration.get(REQUEST_LOGGER_ERROR_ENABLED));
        }
        if (configuration.has(REQUEST_LOGGER_MAX_QUERY_LENGTH)) {
            configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_LOGGER_MAX_QUERY_LENGTH,
                configuration.get(REQUEST_LOGGER_MAX_QUERY_LENGTH));
        }
        if (configuration.has(REQUEST_LOGGER_SHOW_VALUES)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_VALUES,
                configuration.get(REQUEST_LOGGER_SHOW_VALUES));
        }
        if (configuration.has(REQUEST_LOGGER_MAX_VALUE_LENGTH)) {
            configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUE_LENGTH,
                configuration.get(REQUEST_LOGGER_MAX_VALUE_LENGTH));
        }
        if (configuration.has(REQUEST_LOGGER_MAX_VALUES)) {
            configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_LOGGER_MAX_VALUES,
                configuration.get(REQUEST_LOGGER_MAX_VALUES));
        }
        if (configuration.has(REQUEST_LOGGER_SHOW_STACK_TRACES)) {
            configLoaderBuilder.withBoolean(DefaultDriverOption.REQUEST_LOGGER_STACK_TRACES,
                configuration.get(REQUEST_LOGGER_SHOW_STACK_TRACES));
        }
    }
}

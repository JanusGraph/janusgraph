package org.janusgraph.diskstorage.keycolumnvalue.cache;

import org.janusgraph.diskstorage.configuration.Configuration;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_MIN_IDLE_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_POOL_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_CONNECTION_TIME_OUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_HOST;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_KEEP_ALIVE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.REDIS_CACHE_PORT;

public class RedissonCache {

    private static String redisCacheHost;
    private static int redisCachePort;
    private static int connectionPoolSize;
    private static int connectionMinimumIdleSize;
    private static int connectTimeout;
    private static boolean keepAlive;

    public static RedissonClient getRedissonClient(Configuration configuration) {

        redisCacheHost = configuration.get(REDIS_CACHE_HOST);
        redisCachePort = configuration.get(REDIS_CACHE_PORT);
        connectionPoolSize = configuration.get(REDIS_CACHE_CONNECTION_POOL_SIZE);
        connectionMinimumIdleSize = configuration.get(REDIS_CACHE_CONNECTION_MIN_IDLE_SIZE);
        connectTimeout = configuration.get(REDIS_CACHE_CONNECTION_TIME_OUT);
        keepAlive = configuration.get(REDIS_CACHE_KEEP_ALIVE);

        Config config = new Config();
        config.useSingleServer().setAddress("redis://" + redisCacheHost + ":" + redisCachePort).setConnectionPoolSize(connectionPoolSize)
            .setConnectionMinimumIdleSize(connectionMinimumIdleSize)
            .setConnectTimeout(connectTimeout)
            .setKeepAlive(keepAlive);
        return Redisson.create(config);
    }
}

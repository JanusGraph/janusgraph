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

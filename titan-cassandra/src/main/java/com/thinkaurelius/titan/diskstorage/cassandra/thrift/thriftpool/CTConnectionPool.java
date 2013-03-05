package com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import java.util.concurrent.ConcurrentHashMap;

import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.THRIFT_DEFAULT_FRAME_SIZE;
import static com.thinkaurelius.titan.diskstorage.cassandra.AbstractCassandraStoreManager.THRIFT_DEFAULT_MAX_MESSAGE_SIZE;

/**
 * Cassandra-Thrift connection pooler classes using Apache commons-pool.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CTConnectionPool {

    public static final long SCHEMA_WAIT_MAX = 5000;
    public static final long SCHEMA_WAIT_INCREMENT = 25;

    private static final ConcurrentHashMap<PoolKey, CTConnectionFactory>
            factories = new ConcurrentHashMap<PoolKey, CTConnectionFactory>();

    private static final ConcurrentHashMap<PoolKey,
            UncheckedGenericKeyedObjectPool<String, CTConnection>> pools =
            new ConcurrentHashMap<PoolKey,
                    UncheckedGenericKeyedObjectPool<String, CTConnection>>();

    public static void clearPool(String hostname, int port, int timeoutMS) {
        clearPool(hostname, port, timeoutMS, null);
    }

    public static void clearPool(String hostname, int port, int timeoutMS, Object key) {
        UncheckedGenericKeyedObjectPool<String, CTConnection> pool = getPool(hostname,
                                                                             port,
                                                                             timeoutMS,
                                                                             THRIFT_DEFAULT_FRAME_SIZE,
                                                                             THRIFT_DEFAULT_MAX_MESSAGE_SIZE);

        if (key == null)
            pool.clear();
        else
            pool.clear(key);
    }

    /*
     * We don't want to risk creating a pool multiple times, since
     * each one must be explicitly shutdown()d.  Synchronize the method
     * for now to guarantee that only one pool is ever created for a
     * particular PoolKey.
     *
     * TODO: !!! OMG !!! This is very bad to use synchronized like that, should be fixed ASAP.
     */
    public static synchronized UncheckedGenericKeyedObjectPool<String, CTConnection> getPool(String hostname,
                                                                                             int port,
                                                                                             int timeoutMS,
                                                                                             int frameSize,
                                                                                             int maxMessageSize) {
        PoolKey pk = new PoolKey(hostname, port, timeoutMS);

        UncheckedGenericKeyedObjectPool<String, CTConnection> p = pools.get(pk);

        if (null == p) {
            CTConnectionFactory f = getFactory(hostname, port, timeoutMS, frameSize, maxMessageSize);
            p = new UncheckedGenericKeyedObjectPool<String, CTConnection>(f);
            p.setTestOnBorrow(true);
            p.setTestOnReturn(false);
            p.setTestWhileIdle(false);
            p.setWhenExhaustedAction(GenericKeyedObjectPool.WHEN_EXHAUSTED_GROW);
            p.setMaxActive(-1); // "A negative value indicates no limit"
            p.setMaxTotal(-1);
            pools.put(pk, p);
        }

        return p;
    }

    public static CTConnectionFactory getFactory(String hostname, int port, int timeoutMS, int frameSize, int maxMessageSize) {
        PoolKey pk = new PoolKey(hostname, port, timeoutMS);

        CTConnectionFactory f = factories.get(pk);
        if (null == f) {
            f = new CTConnectionFactory(hostname, port, timeoutMS, frameSize, maxMessageSize);
            CTConnectionFactory old = factories.putIfAbsent(pk, f);
            if (null != old)
                f = old;
        }

        return f;
    }

    private static class PoolKey {
        private final String hostname;
        private final int port;
        private final int timeoutMS;

        public PoolKey(String hostname, int port, int timeoutMS) {
            super();
            this.hostname = hostname;
            this.port = port;
            this.timeoutMS = timeoutMS;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + ((hostname == null) ? 0 : hostname.hashCode());
            result = prime * result + port;
            result = prime * result + timeoutMS;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            PoolKey other = (PoolKey) obj;
            if (hostname == null) {
                if (other.hostname != null)
                    return false;
            } else if (!hostname.equals(other.hostname))
                return false;
            if (port != other.port)
                return false;
            if (timeoutMS != other.timeoutMS)
                return false;
            return true;
        }
    }
}




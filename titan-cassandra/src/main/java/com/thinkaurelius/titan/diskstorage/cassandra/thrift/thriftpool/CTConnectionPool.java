package com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool;

import org.apache.commons.pool.impl.GenericKeyedObjectPool;

import java.util.concurrent.ConcurrentHashMap;

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

    /*
     * We don't want to risk creating a pool multiple times, since
     * each one must be explicitly shutdown()d.  Synchronize the method
     * for now to guarantee that only one pool is ever created for a
     * particular PoolKey.
     */
    public static synchronized UncheckedGenericKeyedObjectPool<String, CTConnection>
    getPool(String hostname, int port, int timeoutMS) {
        PoolKey pk = new PoolKey(hostname, port, timeoutMS);

        UncheckedGenericKeyedObjectPool<String, CTConnection> p =
                pools.get(pk);
        if (null == p) {
            CTConnectionFactory f = getFactory(hostname, port, timeoutMS);
            p = new UncheckedGenericKeyedObjectPool<String, CTConnection>(f);
            p.setTestOnBorrow(true);
            p.setTestOnReturn(false);
            p.setTestWhileIdle(false);
            p.setWhenExhaustedAction(
                    GenericKeyedObjectPool.WHEN_EXHAUSTED_GROW);
            p.setMaxActive(-1); // "A negative value indicates no limit"
            p.setMaxTotal(-1);
            pools.put(pk, p);
        }

        return p;
    }

    public static synchronized CTConnectionFactory
    getFactory(String hostname, int port, int timeoutMS) {
        PoolKey pk = new PoolKey(hostname, port, timeoutMS);

        CTConnectionFactory f = factories.get(pk);
        if (null == f) {
            f = new CTConnectionFactory(hostname, port, timeoutMS);
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




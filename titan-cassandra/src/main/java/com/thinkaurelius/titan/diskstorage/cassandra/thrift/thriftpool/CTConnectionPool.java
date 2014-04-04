package com.thinkaurelius.titan.diskstorage.cassandra.thrift.thriftpool;

import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class extends Apache Commons Pool's GenericKeyedObjectPool, adding
 * two methods that support Java 5 generic type safety.  However, a
 * programmer can still cause RuntimeExceptions related to type errors
 * by mixing calls to these additional methods with calls to the legacy
 * "Object"-typed methods.
 * <p/>
 * <p/>
 * Unfortunately, GenericKeyedObjectPool is not actually generic in the
 * type-system sense.  All of its methods are typed to Object, forcing the
 * client programmer to sprinkle code with casts.  This class centralizes
 * that casting to a single method.
 * <p/>
 * <p/>
 * As a corollary, this class is slightly less flexible than
 * GenericKeyedObjectPool, as this class can only store keys and pooled
 * objects each of a single type, whereas GenericKeyedObjectPool could
 * theoretically contain heterogeneous types of each.  However, I do not
 * need the flexibility of heterogeneous types for pooling Thrift
 * connections, the original work that precipitated writing this class.
 *
 * @param <K> Key type
 * @param <V> Pooled object type
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CTConnectionPool extends GenericKeyedObjectPool<String, CTConnection> {
    
    private static final Logger log =
            LoggerFactory.getLogger(CTConnectionPool.class);
    
    public CTConnectionPool(KeyedPoolableObjectFactory<String, CTConnection> factory) {
        super(factory);
    }

    /**
     * If {@code conn} is non-null and is still open, then call
     * {@link GenericKeyedObjectPool#returnObject(String, CTConnection),
     * catching and logging and Exception that method might generate. 
     * This method does not emit any exceptions.
     * 
     * @param keyspace The key of the pooled object being returned
     * @param conn The pooled object being returned, or null to do nothing
     */
    public void returnObjectUnsafe(String keyspace, CTConnection conn) {
        if (conn != null && conn.isOpen()) {
            try {
                returnObject(keyspace, conn);
            } catch (Exception e) {
                log.warn("Failed to return Cassandra connection to pool", e);
                log.warn(
                        "Failure context: keyspace={}, pool={}, connection={}",
                        new Object[] { keyspace, this, conn });
            }
        }
    }
}


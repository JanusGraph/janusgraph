package com.thinkaurelius.titan.diskstorage.cassandra.thriftpool;

import com.thinkaurelius.titan.core.GraphStorageException;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.commons.pool.impl.GenericKeyedObjectPool;

/**
 * This class extends Apache Commons Pool's GenericKeyedObjectPool, adding
 * two methods that support Java 5 generic type safety.  However, a
 * programmer can still cause RuntimeExceptions related to type errors
 * by mixing calls to these additional methods with calls to the legacy
 * "Object"-typed methods.
 * 
 * <p>
 * Unfortunately, GenericKeyedObjectPool is not actually generic in the
 * type-system sense.  All of its methods are typed to Object, forcing the
 * client programmer to sprinkle code with casts.  This class centralizes
 * that casting to a single method.
 *
 * <p>
 * As a corollary, this class is slightly less flexible than
 * GenericKeyedObjectPool, as this class can only store keys and pooled
 * objects each of a single type, whereas GenericKeyedObjectPool could
 * theoretically contain heterogeneous types of each.  However, I do not
 * need the flexibility of heterogeneous types for pooling Thrift
 * connections, the original work that precipitated writing this class.
 * 
 * @author Dan LaRocque <dalaro@hopcount.org>
 *
 * @param <K> Key type
 * @param <V> Pooled object type
 */
public class UncheckedGenericKeyedObjectPool<K,V> extends GenericKeyedObjectPool {

	public UncheckedGenericKeyedObjectPool(KeyedPoolableObjectFactory factory) {
		super(factory);
	}
	
	@Override
	public Object borrowObject(Object key) {
		try {
			return super.borrowObject(key);
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
	}

	@Override
	public void returnObject(Object key, Object o) {
		try {
			super.returnObject(key, o);
		} catch (Exception e) {
			throw new GraphStorageException(e);
		}
	}
	
	/**
	 * This method internally calls {@link #borrowObject(Object)}
	 * and casts the return value to type {@code V}.  Be careful
	 * when mixing calls to this method with calls to superclass
	 * methods that allow the programmer to insert
	 * arbitrarily-typed objects.  That is not recommended, because
	 * the cast in this method will fail if an object which is not
	 * of type V is retrieved by {@link #borrowObject(Object)}.
	 * 
	 * @throws ClassCastException if the programmer permitted pooled
	 *         objects of a type other than V to enter this object and
	 *         tries to borrow one here.
	 * @param key the pool key
	 * @return the pooled object
	 */
	@SuppressWarnings("unchecked")
	public V genericBorrowObject(K key) {
		return (V)borrowObject(key);
	}
	
	public void genericReturnObject(K key, V o) {
		returnObject(key, o);
	}
}
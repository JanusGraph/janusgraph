package com.thinkaurelius.titan.diskstorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.thinkaurelius.titan.StorageSetup;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediator;
import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediatorProvider;
import com.thinkaurelius.titan.exceptions.LockingFailureException;

public abstract class LockKeyColumnValueStoreTest {

	public StorageManager manager;
	public TransactionHandle host1tx1, host1tx2, host2tx1;
	public OrderedKeyColumnValueStore store;

    public static final String dbName = "test";
	
	private ByteBuffer k, c1, c2, v1, v2;
	
	protected final byte[] rid1 = new byte[] { 'a' };
	protected final byte[] rid2 = new byte[] { 'b' };
	
	protected final LocalLockMediatorProvider p1 = new MockupLockMediator();
	protected final LocalLockMediatorProvider p2 = new MockupLockMediator();
	
	@Before
	public void setUp() throws Exception {
        cleanUp();

		open();
		k  = strToByteBuffer("key");
		c1 = strToByteBuffer("col1");
		c2 = strToByteBuffer("col2");
		v1 = strToByteBuffer("val1");
		v2 = strToByteBuffer("val2");
	}
	
	private ByteBuffer strToByteBuffer(String s) throws UnsupportedEncodingException {
		byte[] raw = s.getBytes("UTF-8");
		ByteBuffer b = ByteBuffer.allocate(raw.length);
		b.put(raw).rewind();
		return b;
	}

    public abstract void cleanUp();

    public abstract StorageManager openStorageManager();

    public void open() {
        manager = openStorageManager();
        store = manager.openDatabase(dbName);
        host1tx1 = manager.beginTransaction();
        host1tx2 = manager.beginTransaction();
        host2tx1 = manager.beginTransaction();
    }

	@After
	public void tearDown() throws Exception {
		close();
	}

    public void close() {
        store.close();
        manager.close();
    }
	
	@Test
	public void singleLockAndUnlock() {
		store.acquireLock(k, c1, null, host1tx1);
		store.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		host1tx1.commit();
		
		host1tx1 = manager.beginTransaction();
		assertEquals(v1, store.get(k, c1, host1tx1));
	}
	
	@Test
	public void transactionMayReenterLock() {
		store.acquireLock(k, c1, null, host1tx1);
		store.acquireLock(k, c1, null, host1tx1);
		store.acquireLock(k, c1, null, host1tx1);
		store.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		host1tx1.commit();
		
		host1tx1 = manager.beginTransaction();
		assertEquals(v1, store.get(k, c1, host1tx1));
	}
	
	@Test(expected=LockingFailureException.class)
	public void expectedValueMismatchCausesMutateFailure() {
		store.acquireLock(k, c1, v1, host1tx1);
		store.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		host1tx1.commit();
	}
	
	@Test
	public void testLocalLockContention() {
		store.acquireLock(k, c1, null, host1tx1);
		
		try {
			store.acquireLock(k, c1, null, host1tx2);
			fail("Lock contention exception not thrown");
		} catch (LockingFailureException e) {
			
		}
		
		try {
			store.acquireLock(k, c1, null, host1tx2);
			fail("Lock contention exception not thrown (2nd try)");
		} catch (LockingFailureException e) {
			
		}
		
		host1tx1.commit();
	}
	
	@Test
	public void testRemoteLockContention() throws InterruptedException {
		// acquire lock on "host1"
		store.acquireLock(k, c1, null, host1tx1);
		
		Thread.sleep(50L);
		
		try {
			// acquire same lock on "host2"
			store.acquireLock(k, c1, null, host2tx1);
		} catch (LockingFailureException e) {
			/* Lock attempts between hosts with different LocalLockMediators,
			 * such as host1tx1 and host2tx1 in this example, should
			 * not generate locking failures until one of them tries
			 * to issue a mutate or mutateMany call.  An exception
			 * thrown during the acquireLock call above suggests that
			 * the LocalLockMediators for these two transactions are
			 * not really distinct, which would be a severe and fundamental
			 * bug in this test.
			 */
			fail("Contention between remote transactions detected too soon");
		}
		
		Thread.sleep(50L);
		
		try {
			// This must fail since "host1" took the lock first
			store.mutate(k, Arrays.asList(new Entry(c1, v2)), null, host2tx1);
			fail("Expected lock contention between remote transactions did not occur");
		} catch (LockingFailureException e) {
			
		}
		
		// This should succeed
		store.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		
		host1tx1.commit();
		host1tx1 = manager.beginTransaction();
		assertEquals(v1, store.get(k, c1, host1tx1));
	}
	
	@Test
	public void singleTransactionWithMultipleLocks() {
		
		tryWrites(host1tx1, host1tx1);
	}
	
	@Test
	public void twoLocalTransactionsWithIndependentLocks() {

		tryWrites(host1tx1, host1tx2);
	}
	
	@Test
	public void twoTransactionsWithIndependentLocks() {
		
		tryWrites(host1tx1, host2tx1);
	}
	
	private void tryWrites(TransactionHandle tx1, TransactionHandle tx2) {
		assertNull(store.get(k, c1, tx1));
		assertNull(store.get(k, c2, tx2));
		
		store.acquireLock(k, c1, null, tx1);
		store.acquireLock(k, c2, null, tx2);

		store.mutate(k, Arrays.asList(new Entry(c1, v1)), null, tx1);
		store.mutate(k, Arrays.asList(new Entry(c2, v2)), null, tx2);
		
		tx1.commit();
		if (tx2 != tx1)
			tx2.commit();
		
		TransactionHandle checktx = manager.beginTransaction();
		assertEquals(v1, store.get(k, c1, checktx));
		assertEquals(v2, store.get(k, c2, checktx));
		checktx.commit();
	}
	
	private static class MockupLockMediator implements LocalLockMediatorProvider {
		
		private static final Logger log =
				LoggerFactory.getLogger(MockupLockMediator.class);
		
		private final ConcurrentHashMap<String, LocalLockMediator> mediators =
				new ConcurrentHashMap<String, LocalLockMediator>();

		public LocalLockMediator get(String namespace) {
			LocalLockMediator m = mediators.get(namespace);
			
			if (null == m) {
				m = new LocalLockMediator(namespace);
				LocalLockMediator old = mediators.putIfAbsent(namespace, m);
				if (null != old)
					m = old;
				else 
					log.debug("Local lock mediator instantiated for namespace {}", namespace);
			}
			
			return m;
		}
	}
}

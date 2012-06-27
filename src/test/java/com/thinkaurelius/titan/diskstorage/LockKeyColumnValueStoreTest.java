package com.thinkaurelius.titan.diskstorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import com.thinkaurelius.titan.diskstorage.locking.LocalLockMediators;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class LockKeyColumnValueStoreTest {

	public StorageManager manager1, manager2;
	public TransactionHandle host1tx1, host1tx2, host2tx1;
	public OrderedKeyColumnValueStore store1, store2;

    public static final String dbName = "test";
	
	private ByteBuffer k, c1, c2, v1, v2;
	
	protected final byte[] rid1 = new byte[] { 'a' };
	protected final byte[] rid2 = new byte[] { 'b' };
	
	@Before
	public void setUp() throws Exception {
        openStorageManager((short)1).clearStorage();

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

    public abstract StorageManager openStorageManager(short hostIndex);

    public void open() {
        manager1 = openStorageManager((short)1);
        manager2 = openStorageManager((short)2);
        store1 = manager1.openDatabase(dbName);
        store2 = manager2.openDatabase(dbName);
        host1tx1 = manager1.beginTransaction();
        host1tx2 = manager1.beginTransaction();
        host2tx1 = manager2.beginTransaction();
    }

	@After
	public void tearDown() throws Exception {
		close();
	}

    public void close() {
        store1.close();
        store2.close();
        manager1.close();
        manager2.close();
        LocalLockMediators.INSTANCE.clear();
    }
	
	@Test
	public void singleLockAndUnlock() {
		store1.acquireLock(k, c1, null, host1tx1);
		store1.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		host1tx1.commit();
		
		host1tx1 = manager1.beginTransaction();
		assertEquals(v1, store1.get(k, c1, host1tx1));
	}
	
	@Test
	public void transactionMayReenterLock() {
		store1.acquireLock(k, c1, null, host1tx1);
		store1.acquireLock(k, c1, null, host1tx1);
		store1.acquireLock(k, c1, null, host1tx1);
		store1.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		host1tx1.commit();
		
		host1tx1 = manager1.beginTransaction();
		assertEquals(v1, store1.get(k, c1, host1tx1));
	}
	
	@Test(expected=LockingFailureException.class)
	public void expectedValueMismatchCausesMutateFailure() {
		store1.acquireLock(k, c1, v1, host1tx1);
		store1.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		host1tx1.commit();
	}
	
	@Test
	public void testLocalLockContention() {
		store1.acquireLock(k, c1, null, host1tx1);
		
		try {
			store1.acquireLock(k, c1, null, host1tx2);
			fail("Lock contention exception not thrown");
		} catch (LockingFailureException e) {
			
		}
		
		try {
			store1.acquireLock(k, c1, null, host1tx2);
			fail("Lock contention exception not thrown (2nd try)");
		} catch (LockingFailureException e) {
			
		}
		
		host1tx1.commit();
	}
	
	@Test
	public void testRemoteLockContention() throws InterruptedException {
		// acquire lock on "host1"
		store1.acquireLock(k, c1, null, host1tx1);
		
		Thread.sleep(50L);
		
		try {
			// acquire same lock on "host2"
			store2.acquireLock(k, c1, null, host2tx1);
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
			store2.mutate(k, Arrays.asList(new Entry(c1, v2)), null, host2tx1);
			fail("Expected lock contention between remote transactions did not occur");
		} catch (LockingFailureException e) {
			
		}
		
		// This should succeed
		store1.mutate(k, Arrays.asList(new Entry(c1, v1)), null, host1tx1);
		
		host1tx1.commit();
		host1tx1 = manager1.beginTransaction();
		assertEquals(v1, store1.get(k, c1, host1tx1));
	}
	
	@Test
	public void singleTransactionWithMultipleLocks() {
		
		tryWrites(store1, manager1, host1tx1, store1, host1tx1);
	}
	
	@Test
	public void twoLocalTransactionsWithIndependentLocks() {

		tryWrites(store1, manager1, host1tx1, store1, host1tx2);
	}
	
	@Test
	public void twoTransactionsWithIndependentLocks() {
		
		tryWrites(store1, manager1, host1tx1, store2, host2tx1);
	}
	
	private void tryWrites(OrderedKeyColumnValueStore store1, StorageManager checkmgr,
			TransactionHandle tx1, OrderedKeyColumnValueStore store2,
			TransactionHandle tx2) {
		assertNull(store1.get(k, c1, tx1));
		assertNull(store2.get(k, c2, tx2));
		
		store1.acquireLock(k, c1, null, tx1);
		store2.acquireLock(k, c2, null, tx2);

		store1.mutate(k, Arrays.asList(new Entry(c1, v1)), null, tx1);
		store2.mutate(k, Arrays.asList(new Entry(c2, v2)), null, tx2);
		
		tx1.commit();
		if (tx2 != tx1)
			tx2.commit();
		
		TransactionHandle checktx = checkmgr.beginTransaction();
		assertEquals(v1, store1.get(k, c1, checktx));
		assertEquals(v2, store1.get(k, c2, checktx));
		checktx.commit();
	}
}

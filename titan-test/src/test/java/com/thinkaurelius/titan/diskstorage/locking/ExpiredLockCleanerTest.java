package com.thinkaurelius.titan.diskstorage.locking;

import java.util.List;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.diskstorage.util.ByteBufferUtil;
import com.thinkaurelius.titan.diskstorage.util.KeyColumn;
import com.thinkaurelius.titan.diskstorage.util.StaticArrayBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StorageException;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.Entry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeySliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StaticBufferEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTxConfig;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import com.thinkaurelius.titan.diskstorage.locking.consistentkey.ExpiredLockCleaner;
import com.thinkaurelius.titan.diskstorage.util.TimestampProvider;

import static org.easymock.EasyMock.*;

public class ExpiredLockCleanerTest {

    private IMocksControl ctrl;
    private IMocksControl relaxedCtrl;;
    private ExpiredLockCleaner del;
    private KeyColumnValueStore store;
    private TimestampProvider times;
    private StoreTransaction tx;
    private final KeyColumn kc = new KeyColumn(
            new StaticArrayBuffer(new byte[]{(byte) 1}),
            new StaticArrayBuffer(new byte[]{(byte) 2}));
    private final StaticBuffer defaultLockRid = new StaticArrayBuffer(new byte[]{(byte) 32});
    private final ConsistentKeyLockerSerializer codec = new ConsistentKeyLockerSerializer();
    private final long EXPIRATION_NS = 1000L * 1000L * 1000L;

    @Before
    public void setupMocks() {
        relaxedCtrl = EasyMock.createControl();
        tx = relaxedCtrl.createMock(StoreTransaction.class);
        expect(tx.getConfiguration()).andReturn(new StoreTxConfig()).anyTimes();

        ctrl = EasyMock.createStrictControl();
        times = ctrl.createMock(TimestampProvider.class);
        store = ctrl.createMock(KeyColumnValueStore.class);
        del = new ExpiredLockCleaner(store, kc, times, tx, codec, Long.MAX_VALUE);
    }

    @After
    public void verifyMocks() {
        ctrl.verify();
    }

    @Test
    public void testDeleteSingleExpiredLock() throws StorageException {
        // TODO

        long now = 1L;
        Entry expiredLockCol = new StaticBufferEntry(codec.toLockCol(now,
                defaultLockRid), ByteBufferUtil.getIntBuffer(0));
        List<Entry> expiredSingleton = ImmutableList.<Entry> of(expiredLockCol);

        now += EXPIRATION_NS;

        StaticBuffer key = codec.toLockKey(kc.getKey(), kc.getColumn());
        KeySliceQuery ksq = new KeySliceQuery(key,
                ByteBufferUtil.zeroBuffer(9), ByteBufferUtil.oneBuffer(9));

        expect(store.getSlice(eq(ksq), anyObject(StoreTransaction.class)))
                .andReturn(expiredSingleton);

        store.mutate(
                eq(key),
                eq(ImmutableList.<Entry> of()),
                eq(ImmutableList.<StaticBuffer> of(expiredLockCol.getColumn())),
                anyObject(StoreTransaction.class));

        ctrl.replay();
        del.run();
    }
}

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

package org.janusgraph.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.Locker;
import org.janusgraph.diskstorage.locking.LockerProvider;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStore;
import org.janusgraph.diskstorage.locking.consistentkey.ExpectedValueCheckingStoreManager;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.diskstorage.util.StaticArrayEntryList;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test transaction handling in {@link ExpectedValueCheckingStore} and related
 * classes, particularly with respect to consistency levels offered by the
 * underlying store.
 */
public class ExpectedValueCheckingTest {

    private IMocksControl ctrl;
    private ExpectedValueCheckingStoreManager expectManager;
    private KeyColumnValueStoreManager backingManager;
    private StoreTransaction consistentTx;
    private StoreTransaction inconsistentTx;
    private StoreTransaction expectTx;
    private Locker backingLocker;
    private KeyColumnValueStore backingStore;
    private KeyColumnValueStore expectStore;
    private Capture<BaseTransactionConfig> txConfigCapture;

    private static final String STORE_NAME = "ExpectTestStore";
    private static final String LOCK_SUFFIX = "_expecttest";
    private static final String LOCKER_NAME = STORE_NAME + LOCK_SUFFIX;

    private static final StaticBuffer DATA_KEY = BufferUtil.getIntBuffer(1);
    private static final StaticBuffer DATA_COL = BufferUtil.getIntBuffer(2);
    private static final StaticBuffer DATA_VAL = BufferUtil.getIntBuffer(4);

    private static final StaticBuffer LOCK_KEY = BufferUtil.getIntBuffer(32);
    private static final StaticBuffer LOCK_COL = BufferUtil.getIntBuffer(64);
    private static final StaticBuffer LOCK_VAL = BufferUtil.getIntBuffer(128);

    @BeforeEach
    public void setupMocks() throws BackendException {

        // Initialize mock controller
        ctrl = EasyMock.createStrictControl();
        ctrl.checkOrder(true);

        // Setup some config mocks and objects
        backingManager = ctrl.createMock(KeyColumnValueStoreManager.class);
        LockerProvider lockerProvider = ctrl.createMock(LockerProvider.class);
        ModifiableConfiguration globalConfig = GraphDatabaseConfiguration.buildGraphConfiguration();
        ModifiableConfiguration localConfig = GraphDatabaseConfiguration.buildGraphConfiguration();
        ModifiableConfiguration defaultConfig = GraphDatabaseConfiguration.buildGraphConfiguration();
        // Set some properties on the configs, just so that global/local/default can be easily distinguished
        globalConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "global");
        localConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "local");
        defaultConfig.set(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID, "default");
        BaseTransactionConfig defaultTxConfig = new StandardBaseTransactionConfig.Builder().customOptions(defaultConfig).timestampProvider(TimestampProviders.MICRO).build();
        StoreFeatures backingFeatures = new StandardStoreFeatures.Builder().keyConsistent(globalConfig, localConfig).build();


        // Setup behavior specification starts below this line


        // 1. Construct manager
        // The EVCSManager ctor retrieves the backing store's features and stores it in an instance field
        expect(backingManager.getFeatures()).andReturn(backingFeatures).once();

        // 2. Begin transaction
        // EVCTx begins two transactions on the backingManager: one with globalConfig and one with localConfig
        // The capture is used in the @After method to check the config
        txConfigCapture = EasyMock.newCapture(CaptureType.ALL);
        inconsistentTx = ctrl.createMock(StoreTransaction.class);
        consistentTx = ctrl.createMock(StoreTransaction.class);
        expect(backingManager.beginTransaction(capture(txConfigCapture))).andReturn(inconsistentTx);
        expect(backingManager.beginTransaction(capture(txConfigCapture))).andReturn(consistentTx);

        // 3. Open a database
        backingLocker = ctrl.createMock(Locker.class);
        backingStore = ctrl.createMock(KeyColumnValueStore.class);
        expect(backingManager.openDatabase(STORE_NAME)).andReturn(backingStore);
        expect(backingStore.getName()).andReturn(STORE_NAME);
        expect(lockerProvider.getLocker(LOCKER_NAME)).andReturn(backingLocker);

        // Carry out setup behavior against mocks
        ctrl.replay();
        // 1. Construct manager
        expectManager = new ExpectedValueCheckingStoreManager(backingManager, LOCK_SUFFIX, lockerProvider, Duration.ofSeconds(1L));
        // 2. Begin transaction
        expectTx = expectManager.beginTransaction(defaultTxConfig);
        // 3. Open a database
        expectStore = expectManager.openDatabase(STORE_NAME);

        // Verify behavior and reset the mocks for test methods to use
        ctrl.verify();
        ctrl.reset();
    }

    @AfterEach
    public void verifyMocks() {
        ctrl.verify();
        ctrl.reset();

        // Check capture created in the @Before method
        assertTrue(txConfigCapture.hasCaptured());
        List<BaseTransactionConfig> transactionConfigurations = txConfigCapture.getValues();
        assertEquals(2, transactionConfigurations.size());
        // First backing store transaction should use default tx config
        assertEquals("default", transactionConfigurations.get(0).getCustomOption(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID));
        // Second backing store transaction should use global strong consistency config
        assertEquals("global",  transactionConfigurations.get(1).getCustomOption(GraphDatabaseConfiguration.UNIQUE_INSTANCE_ID));
        // The order in which these transactions are opened isn't really significant;
        // testing them in order is kind of over-specifying the implementation's behavior.
        // Could probably relax the ordering selectively here with some thought, but
        // I want to keep order checking on in general for the EasyMock control.
    }

    @Test
    public void testMutateWithLockUsesConsistentTx() throws BackendException {
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();
        final KeyColumn kc = new KeyColumn(LOCK_KEY, LOCK_COL);

        // 1. Acquire a lock
        backingLocker.writeLock(kc, consistentTx);

        // 2. Run a mutation
        // N.B. mutation coordinates do not overlap with the lock, but consistentTx should be used anyway
        // 2.1. Check locks & expected values before mutating data
        backingLocker.checkLocks(consistentTx);
        StaticBuffer nextBuf = BufferUtil.nextBiggerBuffer(kc.getColumn());
        KeySliceQuery expectedValueQuery = new KeySliceQuery(kc.getKey(), kc.getColumn(), nextBuf);
        expect(backingStore.getSlice(expectedValueQuery, consistentTx)) // expected value read must use strong consistency
            .andReturn(StaticArrayEntryList.of(StaticArrayEntry.of(LOCK_COL, LOCK_VAL)));
        // 2.2. Mutate data
        backingStore.mutate(DATA_KEY, adds, deletions, consistentTx); // writes by txs with locks must use strong consistency

        ctrl.replay();
        // 1. Lock acquisition
        expectStore.acquireLock(LOCK_KEY, LOCK_COL, LOCK_VAL, expectTx);
        // 2. Mutate
        expectStore.mutate(DATA_KEY, adds, deletions, expectTx);
    }

    @Test
    public void testMutateWithoutLockUsesInconsistentTx() throws BackendException {
        // Run a mutation
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();
        backingStore.mutate(DATA_KEY, adds, deletions, inconsistentTx); // consistency level is unconstrained w/o locks

        ctrl.replay();
        expectStore.mutate(DATA_KEY, adds, deletions, expectTx);
    }

    @Test
    public void testMutateManyWithLockUsesConsistentTx() throws BackendException {
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();

        Map<String, Map<StaticBuffer, KCVMutation>> mutations =
                ImmutableMap.of(STORE_NAME,
                        ImmutableMap.of(DATA_KEY, new KCVMutation(adds, deletions)));
        final KeyColumn kc = new KeyColumn(LOCK_KEY, LOCK_COL);

        // Acquire a lock
        backingLocker.writeLock(kc, consistentTx);

        // 2. Run mutateMany
        // 2.1. Check locks & expected values before mutating data
        backingLocker.checkLocks(consistentTx);
        StaticBuffer nextBuf = BufferUtil.nextBiggerBuffer(kc.getColumn());
        KeySliceQuery expectedValueQuery = new KeySliceQuery(kc.getKey(), kc.getColumn(), nextBuf);
        expect(backingStore.getSlice(expectedValueQuery, consistentTx)) // expected value read must use strong consistency
            .andReturn(StaticArrayEntryList.of(StaticArrayEntry.of(LOCK_COL, LOCK_VAL)));
        // 2.2. Run mutateMany on backing manager to modify data
        backingManager.mutateMany(mutations, consistentTx); // writes by txs with locks must use strong consistency

        ctrl.replay();
        // Lock acquisition
        expectStore.acquireLock(LOCK_KEY, LOCK_COL, LOCK_VAL, expectTx);
        // Mutate
        expectManager.mutateMany(mutations, expectTx);
    }

    @Test
    public void testMutateManyWithoutLockUsesInconsistentTx() throws BackendException {
        final ImmutableList<Entry> adds = ImmutableList.of(StaticArrayEntry.of(DATA_COL, DATA_VAL));
        final ImmutableList<StaticBuffer> deletions = ImmutableList.of();

        Map<String, Map<StaticBuffer, KCVMutation>> mutations =
                ImmutableMap.of(STORE_NAME,
                        ImmutableMap.of(DATA_KEY, new KCVMutation(adds, deletions)));

        // Run mutateMany
        backingManager.mutateMany(mutations, inconsistentTx); // consistency level is unconstrained w/o locks

        ctrl.replay();
        // Run mutateMany
        expectManager.mutateMany(mutations, expectTx);
    }
}

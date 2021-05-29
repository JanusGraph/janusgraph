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

package org.janusgraph.diskstorage.locking;

import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLockerSerializer;
import org.janusgraph.diskstorage.locking.consistentkey.LockCleanerService;
import org.janusgraph.diskstorage.locking.consistentkey.StandardLockCleanerRunnable;
import org.janusgraph.diskstorage.locking.consistentkey.StandardLockCleanerService;
import org.janusgraph.diskstorage.util.KeyColumn;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ExecutorService;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;

public class LockCleanerServiceTest {
    private IMocksControl ctrl;
    private KeyColumnValueStore store;
    private StoreTransaction tx;
    private ExecutorService exec;
    private LockCleanerService svc;

    private final ConsistentKeyLockerSerializer codec = new ConsistentKeyLockerSerializer();
    private final KeyColumn kc = new KeyColumn(
            new StaticArrayBuffer(new byte[]{(byte) 1}),
            new StaticArrayBuffer(new byte[]{(byte) 2}));

    @BeforeEach
    public void setupMocks() {
        IMocksControl relaxedCtrl = EasyMock.createControl();
        tx = relaxedCtrl.createMock(StoreTransaction.class);

        ctrl = EasyMock.createStrictControl();
        store = ctrl.createMock(KeyColumnValueStore.class);
        exec = ctrl.createMock(ExecutorService.class);
    }

    @AfterEach
    public void verifyMocks() {
        ctrl.verify();
    }

    @Test
    public void testCleanCooldownBlocksRapidRequests() {
        final Instant cutoff = Instant.ofEpochMilli(1L);

        svc = new StandardLockCleanerService(store, codec, exec, Duration.ofSeconds(60L), TimestampProviders.MILLI);

        expect(exec.submit(eq(new StandardLockCleanerRunnable(store, kc, tx, codec, cutoff, TimestampProviders.MILLI)))).andReturn(null);

        ctrl.replay();

        for (int i = 0; i < 500; i++) {
            svc.clean(kc, cutoff, tx);
        }
    }

    @Test
    public void testCleanCooldownElapses() throws InterruptedException {
        final Instant cutoff = Instant.ofEpochMilli(1L);

        Duration wait = Duration.ofMillis(500L);
        svc = new StandardLockCleanerService(store, codec, exec, wait, TimestampProviders.MILLI);

        expect(exec.submit(eq(new StandardLockCleanerRunnable(store, kc, tx, codec, cutoff, TimestampProviders.MILLI)))).andReturn(null);

        expect(exec.submit(eq(new StandardLockCleanerRunnable(store, kc, tx, codec, cutoff.plusMillis(1), TimestampProviders.MILLI)))).andReturn(null);

        ctrl.replay();

        for (int i = 0; i < 2; i++) {
            svc.clean(kc, cutoff, tx);
        }

        TimestampProviders.MILLI.sleepFor(wait.plusMillis(1));

        for (int i = 0; i < 2; i++) {
            svc.clean(kc, cutoff.plusMillis(1), tx);
        }
    }
}

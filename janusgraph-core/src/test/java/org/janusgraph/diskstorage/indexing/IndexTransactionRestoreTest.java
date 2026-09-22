// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.indexing;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.TemporaryBackendException;
import org.janusgraph.diskstorage.util.StandardBaseTransactionConfig;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

//restore writes whole documents, so it is idempotent and safe to reattempt. Reindex, stale entry removal, CDC index
//updates and transaction log recovery all go through it, and a provider which classifies a failure as temporary
//expects its caller to reattempt, as flushInternal does for mutate
@ExtendWith(MockitoExtension.class)
public class IndexTransactionRestoreTest {

    @Mock
    private IndexProvider index;

    @Mock
    private KeyInformation.IndexRetriever keyInformation;

    @Mock
    private BaseTransactionConfigurable providerTx;

    private final Map<String, Map<String, List<IndexEntry>>> documents = Collections.singletonMap("store",
        Collections.singletonMap("document", Collections.singletonList(new IndexEntry("field", "value"))));

    private IndexTransaction indexTransaction(Duration maxWriteTime) throws BackendException {
        final BaseTransactionConfig config = StandardBaseTransactionConfig.of(TimestampProviders.MILLI);
        when(index.beginTransaction(config)).thenReturn(providerTx);
        return new IndexTransaction(index, keyInformation, config, maxWriteTime);
    }

    @Test
    public void shouldReattemptARestoreWhichFailedTemporarily() throws BackendException {
        final IndexTransaction tx = indexTransaction(Duration.ofSeconds(5));
        doThrow(new TemporaryBackendException("throttled")).doNothing()
            .when(index).restore(documents, keyInformation, providerTx);

        tx.restore(documents);

        verify(index, times(2)).restore(documents, keyInformation, providerTx);
    }

    @Test
    public void shouldNotReattemptARestoreWhichFailedPermanently() throws BackendException {
        final IndexTransaction tx = indexTransaction(Duration.ofSeconds(5));
        doThrow(new PermanentBackendException("rejected")).when(index).restore(documents, keyInformation, providerTx);

        assertThrows(PermanentBackendException.class, () -> tx.restore(documents));

        verify(index, times(1)).restore(documents, keyInformation, providerTx);
    }

    @Test
    public void shouldGiveUpOnceTheWriteTimeIsExhausted() throws BackendException {
        //That a reattempt happens at all is the previous test's; this one only asks how the wait ends, so the budget
        //is short and the number of attempts within it is not asserted, which keeps timing out of the test
        final IndexTransaction tx = indexTransaction(Duration.ofMillis(200));
        doThrow(new TemporaryBackendException("throttled")).when(index).restore(documents, keyInformation, providerTx);

        //The exception which ends the wait is temporary as well, so a caller can still tell it from a rejection
        final TemporaryBackendException e = assertThrows(TemporaryBackendException.class, () -> tx.restore(documents));
        assertTrue(e.getMessage().contains("repeated temporary exceptions"), e.getMessage());

        verify(index, atLeastOnce()).restore(documents, keyInformation, providerTx);
    }
}

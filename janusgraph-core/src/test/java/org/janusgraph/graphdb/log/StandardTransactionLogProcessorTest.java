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

package org.janusgraph.graphdb.log;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LONGEST_RECOVERY_WAIT;
import static org.janusgraph.graphdb.log.StandardTransactionLogProcessor.epochNanos;
import static org.janusgraph.graphdb.log.StandardTransactionLogProcessor.nanosUntilExpiry;
import static org.junit.jupiter.api.Assertions.assertEquals;

//Recovery gives a transaction up once the transaction log has been read past max-commit-time after the transaction's
//first entry, by a nanosecond, the read progress being exclusive, however long reading takes, and still waits about
//146 years at most.
public class StandardTransactionLogProcessorTest {

    private static final Instant START = Instant.parse("2026-09-25T12:00:00.000123Z");

    @Test
    public void shouldExpireOnceTheLogIsReadUpToMaxCommitTimePastTheFirstEntry() {
        final Duration maxCommitTime = Duration.ofSeconds(300);
        assertEquals(Duration.ofSeconds(300).toNanos() + 1, nanosUntilExpiry(START, maxCommitTime, epochNanos(START)));
        assertEquals(Duration.ofSeconds(200).toNanos() + 1,
            nanosUntilExpiry(START, maxCommitTime, epochNanos(START.plusSeconds(100))));
        //The log was read up to before the entry was written, as it is while the read which found it is processed
        assertEquals(Duration.ofSeconds(310).toNanos() + 1,
            nanosUntilExpiry(START, maxCommitTime, epochNanos(START.minusSeconds(10))));
        //Read up to the deadline itself, an entry written at it still unread: one nanosecond more to read
        assertEquals(1, nanosUntilExpiry(START, maxCommitTime, epochNanos(START.plusSeconds(300))));
        assertEquals(0, nanosUntilExpiry(START, maxCommitTime, epochNanos(START.plusSeconds(300).plusNanos(1))));
        assertEquals(0, nanosUntilExpiry(START, maxCommitTime, epochNanos(START.plusSeconds(400))));
    }

    @Test
    public void shouldWaitNoLongerThanRecoveryCan() {
        assertEquals(LONGEST_RECOVERY_WAIT.toNanos(),
            nanosUntilExpiry(START, Duration.ofDays(365L * 1000), epochNanos(START)));
        //The longest Duration there is: capped without being converted to nanoseconds, which would overflow
        assertEquals(LONGEST_RECOVERY_WAIT.toNanos(),
            nanosUntilExpiry(START, Duration.ofSeconds(Long.MAX_VALUE, 999_999_999), epochNanos(START)));
        assertEquals(LONGEST_RECOVERY_WAIT.toNanos(),
            nanosUntilExpiry(START, LONGEST_RECOVERY_WAIT, epochNanos(START.minusSeconds(10))));
        //A first entry two centuries past the progress: the sum would wrap around without the guard
        assertEquals(LONGEST_RECOVERY_WAIT.toNanos(),
            nanosUntilExpiry(START.plus(Duration.ofDays(365L * 200)), LONGEST_RECOVERY_WAIT, epochNanos(START)));
    }

    @Test
    public void shouldCountNanosecondsSinceTheEpoch() {
        assertEquals(0, epochNanos(Instant.EPOCH));
        assertEquals(1_000_000_123L, epochNanos(Instant.ofEpochSecond(1, 123)));
    }
}

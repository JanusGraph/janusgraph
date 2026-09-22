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

package org.janusgraph.graphdb.configuration;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LONGEST_RECOVERY_WAIT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MAX_COMMIT_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_WRITE_WAITTIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.outlastsASingleChunkCommit;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.singleChunkCommitBudget;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.suggestedManagementSystemCall;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

//Transaction recovery considers a transaction failed once max-commit-time has elapsed since it read the transaction's
//first log entry, while the commit may still be reattempting its writes in turn - the storage write, then one per
//index backend - each for up to write-time. The transaction log's own writes add nothing for the index documents: the
//first entry is written before recovery can read it, the primary success is committed with the storage write, and the
//secondary status follows the index writes; only a transaction which writes a user log needs more. That budget is the least a commit may take: several storage chunks, a separate storage
//write of new schema elements on a backend without transaction isolation, the preparation of the writes and the final
//attempt of each write only add to it. A graph whose max-commit-time does not outlast even a
//commit whose storage write is a single chunk is warned at open; these pin when that is, and that the default is not
//such a value for a graph with one index backend.
public class MaxCommitTimeTest {

    private static final Duration WRITE_TIME = Duration.ofSeconds(100);

    @Test
    public void defaultShouldOutlastACommitWithOneIndexBackendWithAWriteTimeToSpare() {
        //The default used to be 10 s against a write time of 100 s, which outlasted not even the storage write.
        final Duration maxCommitTime = MAX_COMMIT_TIME.getDefaultValue();
        final Duration writeTime = STORAGE_WRITE_WAITTIME.getDefaultValue();
        assertTrue(outlastsASingleChunkCommit(maxCommitTime, writeTime, 1),
            () -> MAX_COMMIT_TIME.toStringWithoutRoot() + " defaults to " + maxCommitTime + ", which does not outlast "
                + "the storage write and one index write of a commit at " + writeTime + " each");
        assertTrue(maxCommitTime.compareTo(writeTime.multipliedBy(3)) >= 0,
            () -> MAX_COMMIT_TIME.toStringWithoutRoot() + " defaults to " + maxCommitTime + ", which leaves no write "
                + "time to spare for a second storage chunk or the final attempts of those writes");
    }

    @Test
    public void shouldOutlastOnlyACommitWhichTakesLess() {
        assertTrue(outlastsASingleChunkCommit(Duration.ofSeconds(300), WRITE_TIME, 1));
        assertFalse(outlastsASingleChunkCommit(Duration.ofSeconds(10), WRITE_TIME, 0));
    }

    @Test
    public void shouldNotOutlastACommitBudgetOfExactlyTheSameLength() {
        //The storage write and two index writes at 100 s each: the last attempt of each can start just before its
        //write time runs out, so 300 s is not enough.
        assertEquals(Optional.of(Duration.ofSeconds(300)), singleChunkCommitBudget(WRITE_TIME, 2));
        assertFalse(outlastsASingleChunkCommit(Duration.ofSeconds(300), WRITE_TIME, 2));
    }

    @Test
    public void shouldHaveNoBudgetForAWriteTimeTooLongToMultiply() {
        //No max-commit-time can outlast it, which the warning says instead of naming a value to set.
        assertEquals(Optional.empty(), singleChunkCommitBudget(Duration.ofSeconds(Long.MAX_VALUE), 1));
        assertFalse(outlastsASingleChunkCommit(Duration.ofSeconds(300), Duration.ofSeconds(Long.MAX_VALUE), 1));
    }

    @Test
    public void shouldSuggestAManagementSystemCallWhichCanBePastedAsItIs() {
        //The budget and one write time more: the default, for the default write time and one index backend.
        assertEquals("mgmt.set(\"tx.max-commit-time\", java.time.Duration.parse(\"PT5M\"))",
            suggestedManagementSystemCall(STORAGE_WRITE_WAITTIME.getDefaultValue(), 1));
        assertEquals(MAX_COMMIT_TIME.getDefaultValue(), Duration.parse("PT5M"));
        //With two index backends at 100 s: 400 s, which outlasts the 300 s of their budget.
        assertEquals("mgmt.set(\"tx.max-commit-time\", java.time.Duration.parse(\"PT6M40S\"))",
            suggestedManagementSystemCall(WRITE_TIME, 2));
        assertTrue(outlastsASingleChunkCommit(Duration.parse("PT6M40S"), WRITE_TIME, 2));
        //Without a budget, which the warning reports instead of suggesting a value, the call keeps a placeholder.
        assertEquals("mgmt.set(\"tx.max-commit-time\", <duration>)",
            suggestedManagementSystemCall(Duration.ofSeconds(Long.MAX_VALUE / 2), 1));
    }

    @Test
    public void shouldHaveNoBudgetLongerThanRecoveryCanWait() {
        //Recovery waits about 146 years at most. A Duration holds a budget of 200 years, but no value recovery can wait
        //for exceeds it.
        final Duration hundredYears = Duration.ofDays(365L * 100);
        assertEquals(Optional.of(hundredYears), singleChunkCommitBudget(hundredYears, 0));
        assertEquals(Optional.empty(), singleChunkCommitBudget(hundredYears, 1));
        assertFalse(outlastsASingleChunkCommit(Duration.ofDays(365L * 1000), hundredYears, 1));
        //A budget of exactly the longest wait cannot be exceeded either; a nanosecond less can.
        assertEquals(Optional.empty(), singleChunkCommitBudget(LONGEST_RECOVERY_WAIT, 0));
        assertEquals(Optional.of(LONGEST_RECOVERY_WAIT.minusNanos(1)),
            singleChunkCommitBudget(LONGEST_RECOVERY_WAIT.minusNanos(1), 0));
    }

    @Test
    public void shouldSuggestNoMoreThanRecoveryCanWait() {
        //The budget and one write time would be 200 years. Recovery waits about 146 at most, which still exceeds the
        //budget, so that is the value suggested.
        final Duration hundredYears = Duration.ofDays(365L * 100);
        assertEquals("mgmt.set(\"tx.max-commit-time\", java.time.Duration.parse(\"PT1281023H53M38.427387903S\"))",
            suggestedManagementSystemCall(hundredYears, 0));
        final Duration suggested = Duration.parse("PT1281023H53M38.427387903S");
        assertEquals(LONGEST_RECOVERY_WAIT, suggested);
        assertEquals(Long.MAX_VALUE >>> 1, suggested.toNanos());
        assertTrue(outlastsASingleChunkCommit(suggested, hundredYears, 0));
        //Even a write time a nanosecond short of the longest wait, whose budget and one write time come to nearly
        //twice it, is summed without an overflow: a Duration holds billions of times as much. It is capped as well.
        assertEquals("mgmt.set(\"tx.max-commit-time\", java.time.Duration.parse(\"PT1281023H53M38.427387903S\"))",
            suggestedManagementSystemCall(LONGEST_RECOVERY_WAIT.minusNanos(1), 0));
    }
}

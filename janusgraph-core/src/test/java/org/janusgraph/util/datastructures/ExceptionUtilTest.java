// Copyright 2023 JanusGraph Authors
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

package org.janusgraph.util.datastructures;

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExceptionUtilTest {

    @Test
    public void isCausedBy_WhenExceptionHasNoCause_ReturnsFalse() {
        Throwable exception = new RuntimeException("Test exception");
        boolean result = ExceptionUtil.isCausedBy(exception, NullPointerException.class);
        assertFalse(result);
    }

    @Test
    public void isCausedBy_WhenExceptionCauseMatches_ReturnsTrue() {
        NullPointerException cause = new NullPointerException("Test cause");
        Throwable exception = new RuntimeException("Test exception", cause);
        boolean result = ExceptionUtil.isCausedBy(exception, NullPointerException.class);
        assertTrue(result);
    }

    @Test
    public void isCausedBy_WhenExceptionCauseDoesNotMatch_ReturnsFalse() {
        IllegalArgumentException cause = new IllegalArgumentException("Test cause");
        Throwable exception = new RuntimeException("Test exception", cause);
        boolean result = ExceptionUtil.isCausedBy(exception, NullPointerException.class);
        assertFalse(result);
    }

    @Test
    public void convertIfInterrupted_WhenRuntimeExceptionNotCausedByInterruptedException_ReturnsSameException() {
        RuntimeException exception = new RuntimeException("Test exception");
        RuntimeException result = ExceptionUtil.convertIfInterrupted(exception);
        assertSame(exception, result);
    }

    @Test
    public void convertIfInterrupted_WhenRuntimeExceptionCausedByInterruptedException_ReturnsTraversalInterruptedException() {
        InterruptedException cause = new InterruptedException("Test cause");
        RuntimeException exception = new RuntimeException("Test exception", cause);
        RuntimeException result = ExceptionUtil.convertIfInterrupted(exception);
        assertTrue(result instanceof TraversalInterruptedException);
        assertEquals(exception, result.getCause());
    }
}

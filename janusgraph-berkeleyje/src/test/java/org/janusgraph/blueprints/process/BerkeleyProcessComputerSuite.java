// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.blueprints.process;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.ProcessComputerSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalInterruptionComputerTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

import java.lang.reflect.Field;

/**
 * Custom TinkerPop {@link ProcessComputerSuite} that excludes {@link TraversalInterruptionComputerTest} for compatibility with
 * BerkeleyDB JE, which does not support thread interrupts.
 */
public class BerkeleyProcessComputerSuite extends ProcessComputerSuite {

    public BerkeleyProcessComputerSuite(final Class<?> classToTest, final RunnerBuilder builder) throws InitializationError {
        super(classToTest, builder, getTestList());
    }

    private static Class<?>[] getTestList() throws InitializationError {
        try {
            final Field field = ProcessComputerSuite.class.getDeclaredField("allTests");
            field.setAccessible(true);
            return (Class<?>[]) ArrayUtils.removeElement((Class<?>[]) field.get(null), TraversalInterruptionComputerTest.class);
        } catch (ReflectiveOperationException e) {
            throw new InitializationError("Unable to create test list");
        }
    }
}

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

package org.janusgraph.core.attribute;

import org.apache.tinkerpop.gremlin.process.traversal.Text;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.janusgraph.graphdb.query.JanusGraphPredicate;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TinkerPopTextWrappingPredicateTest {

    @Test
    public void shouldReturnWrappedTestResultForTest() {
        TinkerPopTextWrappingPredicate wrappingPredicate = new TinkerPopTextWrappingPredicate(Text.containing);

        assertEquals(TextP.containing("ark").test("marko"), wrappingPredicate.test("marko", "ark"));
    }

    @Test
    public void shouldReturnNegatedWrappedTestResultForTestAfterNegate() {
        TinkerPopTextWrappingPredicate wrappingPredicate = new TinkerPopTextWrappingPredicate(Text.containing);

        JanusGraphPredicate negated = wrappingPredicate.negate();

        assertEquals(TextP.containing("ark").negate().test("marko"), negated.test("marko", "ark"));
    }

    @Test
    public void shouldReportNonEmptyStringAsValidCondition() {
        TinkerPopTextWrappingPredicate wrappingPredicate = createWrappingPredicate();

        assertTrue(wrappingPredicate.isValidCondition("nonEmpty"));
    }

    @Test
    public void shouldReportNullAsInvalidCondition() {
        TinkerPopTextWrappingPredicate wrappingPredicate = createWrappingPredicate();

        assertFalse(wrappingPredicate.isValidCondition(null));
    }

    @Test
    public void shouldReportEmptyStringAsInvalidCondition() {
        TinkerPopTextWrappingPredicate wrappingPredicate = createWrappingPredicate();

        assertFalse(wrappingPredicate.isValidCondition(""));
    }

    @Test
    public void shouldReportNonStringAsInvalidCondition() {
        TinkerPopTextWrappingPredicate wrappingPredicate = createWrappingPredicate();

        assertFalse(wrappingPredicate.isValidCondition(1));
    }

    private TinkerPopTextWrappingPredicate createWrappingPredicate() {
        return new TinkerPopTextWrappingPredicate(Text.containing);
    }
}

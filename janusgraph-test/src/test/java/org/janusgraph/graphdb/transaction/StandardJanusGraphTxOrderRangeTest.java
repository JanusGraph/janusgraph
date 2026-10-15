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

package org.janusgraph.graphdb.transaction;

import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.ParameterType;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import static org.janusgraph.graphdb.transaction.StandardJanusGraphTx.isRangedAsOrdered;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * When a graph query whose new elements fill its limit may narrow the index query by a range on its first order key:
 * only where every index compares the bound with the field's values as memory orders them.
 */
public class StandardJanusGraphTxOrderRangeTest {

    private final PropertyKey key = mock(PropertyKey.class);

    private ParameterIndexField field(SchemaStatus status, Parameter... parameters) {
        final Parameter[] all = Arrays.copyOf(parameters, parameters.length + 1);
        all[parameters.length] = ParameterType.STATUS.getParameter(status);
        return ParameterIndexField.of(key, all);
    }

    @Test
    public void takesARangeForWholeNumbersAndDatesInBothDirections() {
        final ParameterIndexField field = field(SchemaStatus.ENABLED);
        for (final Object value : Arrays.asList(1L, 1, (short) 1, (byte) 1, new Date(0),
                Instant.ofEpochSecond(1, 5_000_000))) {
            assertTrue(isRangedAsOrdered(value, field, true), value::toString);
            assertTrue(isRangedAsOrdered(value, field, false), value::toString);
        }
    }

    @Test
    public void takesARangeForDecimalsOnlyInAnAscendingOrder() {
        final ParameterIndexField field = field(SchemaStatus.ENABLED);
        assertTrue(isRangedAsOrdered(1.5, field, false));
        assertTrue(isRangedAsOrdered(1.5f, field, false));
        assertFalse(isRangedAsOrdered(1.5, field, true));
        assertFalse(isRangedAsOrdered(1.5f, field, true));
        assertFalse(isRangedAsOrdered(Double.NaN, field, false));
        assertFalse(isRangedAsOrdered(Float.NaN, field, false));
    }

    @Test
    public void takesARangeForAStringOnlyOnAStringFieldWithoutAnAnalyzer() {
        final ParameterIndexField string = field(SchemaStatus.ENABLED, Mapping.STRING.asParameter());
        assertTrue(isRangedAsOrdered("m", string, true));
        assertTrue(isRangedAsOrdered("m", string, false));
        assertTrue(isRangedAsOrdered("", string, true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, Mapping.TEXTSTRING.asParameter()), true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, Mapping.TEXT.asParameter()), true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED), true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, Mapping.STRING.asParameter(),
            ParameterType.STRING_ANALYZER.getParameter("keyword")), true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, Mapping.STRING.asParameter(),
            ParameterType.TEXT_ANALYZER.getParameter("standard")), true));
    }

    @Test
    public void takesNoRangeForAStringWithACharacterFromTheSurrogatesUp() {
        final ParameterIndexField string = field(SchemaStatus.ENABLED, Mapping.STRING.asParameter());
        assertTrue(isRangedAsOrdered("\uD7FF", string, true));
        assertFalse(isRangedAsOrdered("\uD800", string, true));
        assertFalse(isRangedAsOrdered("a\uE000", string, false));
        assertFalse(isRangedAsOrdered("\uFFFF", string, true));
        assertFalse(isRangedAsOrdered("a\uD83D\uDE00", string, true));
    }

    @Test
    public void takesNoRangeOnAFieldWhichIsntEnabledOrHasCustomParameters() {
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.REGISTERED), true));
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.INSTALLED), true));
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.DISABLED), true));
        assertFalse(isRangedAsOrdered(1L, field(SchemaStatus.ENABLED,
            Parameter.of(ParameterType.CUSTOM_PARAMETER_PREFIX + "index", false)), true));
        assertFalse(isRangedAsOrdered("m", field(SchemaStatus.ENABLED, Mapping.STRING.asParameter(),
            Parameter.of(ParameterType.CUSTOM_PARAMETER_PREFIX + "normalizer", "lowercase")), true));
    }

    @Test
    public void takesNoRangeForOtherTypesOrAnInstantFinerThanMilliseconds() {
        final ParameterIndexField field = field(SchemaStatus.ENABLED);
        for (final Object value : Arrays.asList(true, UUID.randomUUID(), 'm', new BigDecimal("1.5"),
                Geoshape.point(1, 2), Instant.ofEpochSecond(1, 5))) {
            assertFalse(isRangedAsOrdered(value, field, true), value::toString);
            assertFalse(isRangedAsOrdered(value, field, false), value::toString);
        }
    }
}

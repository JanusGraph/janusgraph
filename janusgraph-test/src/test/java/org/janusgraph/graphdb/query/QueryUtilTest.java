// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.query;

import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class QueryUtilTest {

    @Mock
    private StandardJanusGraphTx tx;

    @Test
    void testAdjustLimitForTxModifications() {
        Mockito.when(tx.hasModifications()).thenReturn(true);

        assertEquals(1005, QueryUtil.adjustLimitForTxModifications(tx, 0, 1000));
        assertEquals(100005, QueryUtil.adjustLimitForTxModifications(tx, 0, 100000));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 0, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 0, Integer.MAX_VALUE - 1));
        assertEquals(Integer.MAX_VALUE / 2 + 5, QueryUtil.adjustLimitForTxModifications(tx, 0, Integer.MAX_VALUE / 2));

        assertEquals(2005, QueryUtil.adjustLimitForTxModifications(tx, 1, 1000));
        assertEquals(200005, QueryUtil.adjustLimitForTxModifications(tx, 1, 100000));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 1, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 1, Integer.MAX_VALUE - 1));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 1, Integer.MAX_VALUE / 2));

        assertEquals(1024005, QueryUtil.adjustLimitForTxModifications(tx, 10, 1000));
        assertEquals(102400005, QueryUtil.adjustLimitForTxModifications(tx, 10, 100000));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 10, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 10, Integer.MAX_VALUE - 1));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 10, Integer.MAX_VALUE / 2));

        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 30, 100000));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 30, Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 30, Integer.MAX_VALUE - 1));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 30, Integer.MAX_VALUE / 2));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 100, 100000));

        Mockito.when(tx.hasModifications()).thenReturn(false);

        assertEquals(1000, QueryUtil.adjustLimitForTxModifications(tx, 0, 1000));
        assertEquals(100000, QueryUtil.adjustLimitForTxModifications(tx, 0, 100000));
        assertEquals(2000, QueryUtil.adjustLimitForTxModifications(tx, 1, 1000));
        assertEquals(200000, QueryUtil.adjustLimitForTxModifications(tx, 1, 100000));
        assertEquals(Integer.MAX_VALUE, QueryUtil.adjustLimitForTxModifications(tx, 10, Integer.MAX_VALUE));
    }
}

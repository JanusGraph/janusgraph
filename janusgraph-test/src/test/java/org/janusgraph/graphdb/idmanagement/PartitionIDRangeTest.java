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

package org.janusgraph.graphdb.idmanagement;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.util.BufferUtil;
import org.janusgraph.graphdb.database.idassigner.placement.PartitionIDRange;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class PartitionIDRangeTest {


    @Test
    public void basicIDRangeTest() {
        PartitionIDRange pr;

        for (int[] bounds : new int[][]{{0,16},{5,5},{9,9},{0,0}}) {
            pr = new PartitionIDRange(bounds[0], bounds[1], 16);
            Set<Integer> allIds = Sets.newHashSet(Arrays.asList(ArrayUtils.toObject(pr.getAllContainedIDs())));
            assertEquals(16, allIds.size());
            for (int i = 0; i < 16; i++) {
                assertTrue(allIds.contains(i));
                assertTrue(pr.contains(i));
            }
            assertFalse(pr.contains(16));
            verifyRandomSampling(pr);
        }

        pr = new PartitionIDRange(13,2,16);
        assertTrue(pr.contains(15));
        assertTrue(pr.contains(1));
        assertEquals(5,pr.getAllContainedIDs().length);
        verifyRandomSampling(pr);

        pr = new PartitionIDRange(512,2,2048);
        assertEquals(2048-512+2,pr.getAllContainedIDs().length);
        verifyRandomSampling(pr);

        pr = new PartitionIDRange(512,1055,2048);
        assertEquals(1055-512,pr.getAllContainedIDs().length);
        verifyRandomSampling(pr);

        try {
            pr = new PartitionIDRange(0,5,4);
            fail();
        } catch (IllegalArgumentException ignored) {}
        try {
            pr = new PartitionIDRange(5,3,4);
            fail();
        } catch (IllegalArgumentException ignored) {}
        try {
            pr = new PartitionIDRange(-1,3,4);
            fail();
        } catch (IllegalArgumentException ignored) {}
    }

    private void verifyRandomSampling(PartitionIDRange pr) {
        Set<Integer> allIds = Sets.newHashSet(Arrays.asList(ArrayUtils.toObject(pr.getAllContainedIDs())));
        /* Verify that the probability of NOT sampling the whole space is exceedingly small
        The probability of NOT sampling (with replacement) a single element from a set of x elements in T trials is:
         ((x-1)/x)^T
         Hence, an upper bound for not sampling any of the elements (assuming independence turning OR into +) is:
         x * ((x-1)/x)^T
        */
        double x = allIds.size();
        final int T = 300000;
        double failureSampleProblem = x * Math.pow((x-1)/x,T);
        assertTrue(failureSampleProblem<1e-12); //Make sure the failure prob is infinitesimally small

        Set<Integer> randomIds = Sets.newHashSet();
        for (int t=0;t<T;t++) {
            int id = pr.getRandomID();
            randomIds.add(id);
            assertTrue(allIds.contains(id));
        }
        assertEquals(allIds.size(),randomIds.size());

    }

    @Test
    public void convertIDRangesFromBits() {
        PartitionIDRange pr;

        for (int partitionBits : new int[]{0,1,4,16,5,7,2}) {
            pr = Iterables.getOnlyElement(PartitionIDRange.getGlobalRange(partitionBits));
            assertEquals(1<<partitionBits,pr.getUpperID());
            assertEquals(1<<partitionBits,pr.getAllContainedIDs().length);
            if (partitionBits<=10) verifyRandomSampling(pr);
        }

        try {
            PartitionIDRange.getGlobalRange(-1);
            fail();
        } catch (IllegalArgumentException ignored) {}
    }

    @Test
    public void convertIDRangesFromBuffers() {
        PartitionIDRange pr;

        pr = getPIR(2,2,6,3);
        assertEquals(2, pr.getAllContainedIDs().length);
        assertTrue(pr.contains(1));
        assertTrue(pr.contains(2));
        assertFalse(pr.contains(3));
        pr = getPIR(2,3,6,3);
        assertEquals(1, pr.getAllContainedIDs().length);
        assertFalse(pr.contains(1));
        assertTrue(pr.contains(2));
        assertFalse(pr.contains(3));
        pr = getPIR(4,2,6,3);
        assertEquals(8, pr.getAllContainedIDs().length);
        pr = getPIR(2,6,6,3);
        assertEquals(4,pr.getAllContainedIDs().length);
        pr = getPIR(2,7,7,3);
        assertEquals(4,pr.getAllContainedIDs().length);
        pr = getPIR(2,10,9,4);
        assertEquals(3,pr.getAllContainedIDs().length);
        pr = getPIR(2,5,15,4);
        assertEquals(1, pr.getAllContainedIDs().length);
        pr = getPIR(2,9,16,4);
        assertEquals(1, pr.getAllContainedIDs().length);
        assertTrue(pr.contains(3));

        assertNull(getPIR(2, 11, 12, 4));
        assertNull(getPIR(2, 5, 11, 4));
        assertNull(getPIR(2, 9, 12, 4));
        assertNull(getPIR(2, 9, 11, 4));
        assertNull(getPIR(2, 13, 15, 4));
        assertNull(getPIR(2, 13, 3, 4));


        pr = getPIR(2,15,14,4);
        assertEquals(3,pr.getAllContainedIDs().length);

        pr = getPIR(1,7,6,3);
        assertEquals(1, pr.getAllContainedIDs().length);
        assertTrue(pr.contains(0));
    }


    public static PartitionIDRange getPIR(int partitionBits, long lower, long upper, int bitWidth) {
        return Iterables.getOnlyElement(PartitionIDRange.getIDRanges(partitionBits, convert(lower, upper, bitWidth)), null);
    }

    public static List<KeyRange> convert(long lower, long upper, int bitWidth) {
        StaticBuffer lowerBuffer = BufferUtil.getLongBuffer(convert(lower, bitWidth));
        StaticBuffer upperBuffer = BufferUtil.getLongBuffer(convert(upper, bitWidth));
//        Preconditions.checkArgument(lowerBuffer.compareTo(upperBuffer) < 0, "%s vs %s",lowerBuffer,upperBuffer);
        return Lists.newArrayList(new KeyRange(lowerBuffer, upperBuffer));
    }

    public static long convert(long id, int bitWidth) {
        Preconditions.checkArgument(id>=0 && id<=(1<<bitWidth));
        return id<<(64-bitWidth);
    }

}

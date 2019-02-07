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

package org.janusgraph.util.datastructures;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class RandomRemovalListTest {

    private static final Logger log =
            LoggerFactory.getLogger(RandomRemovalListTest.class);

    @Test
    public void test1() {
        int max = 1000000;
        final RandomRemovalList<Integer> list = new RandomRemovalList<>();
        for (int i = 1; i <= max; i++) {
            list.add(i);
        }

        long sum = 0;
        int subset = max / 10;
        for (int j = 1; j <= subset; j++) {
            sum += list.getRandom();
        }
        double avg = sum / (double) subset;
        log.debug("Average: {}", avg);
        assertEquals(avg, (double) max / 2, max / 100);

    }

    @Test
    public void test2() {
        runIndividual();
    }

    public int runIndividual() {
        long max = 2000000;
        final RandomRemovalList<Integer> list = new RandomRemovalList<>();
        for (int i = 1; i <= max; i++) {
            list.add(i);
        }

        long sum = 0;
        for (int j = 1; j <= max; j++) {
            sum += list.getRandom();
        }
        assertEquals(sum, (max + 1) * max / 2);
        return list.getNumCompactions();
    }

    @Test
    public void test3() {
        long max = 20000;
        final RandomRemovalList<Integer> list = new RandomRemovalList<>();
        for (int i = 1; i <= max; i++) {
            list.add(i);
        }

        long sum = 0;
        int numReturned = 0;
        for (final Integer aList : list) {
            sum += aList;
            numReturned++;
        }
        assertEquals(sum, (max + 1) * max / 2);
        assertEquals(numReturned, max);
    }

    //@Test
    public void benchmark() {
        for (int i = 10; i < 20; i = i + 1) {
            //RandomRemovalList.numTriesBeforeCompaction=i;
            long before = System.currentTimeMillis();
            int compact = runIndividual();
            long after = System.currentTimeMillis();
            log.debug(i + " : " + ((after - before)) + " : " + compact);
            System.gc();
        }
    }

}

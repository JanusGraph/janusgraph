package com.thinkaurelius.titan.util.datastructures;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;


public class RandomRemovalListTest {

    private static final Logger log =
            LoggerFactory.getLogger(RandomRemovalListTest.class);

    @Test
    public void test1() {
        int max = 1000000;
        RandomRemovalList<Integer> list = new RandomRemovalList<Integer>();
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
        RandomRemovalList<Integer> list = new RandomRemovalList<Integer>();
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
        RandomRemovalList<Integer> list = new RandomRemovalList<Integer>();
        for (int i = 1; i <= max; i++) {
            list.add(i);
        }

        long sum = 0;
        int numReturned = 0;
        Iterator<Integer> iter = list.iterator();
        while (iter.hasNext()) {
            sum += iter.next();
            numReturned++;
        }
        assertEquals(sum, (max + 1) * max / 2);
        assertEquals(numReturned, max);
    }

    //@Test
    public void benchmark() {
        for (int i = 10; i < 20; i = i + 1) {
            //RandomRemovalList.numTriesBeforeCompactification=i;
            long before = System.currentTimeMillis();
            int compact = runIndividual();
            long after = System.currentTimeMillis();
            log.debug(i + " : " + ((after - before)) + " : " + compact);
            System.gc();
        }
    }

}

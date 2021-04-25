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

package org.janusgraph.testutil;

import com.google.common.base.Preconditions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RandomGenerator {

    private static final Logger log =
            LoggerFactory.getLogger(RandomGenerator.class);

    private static final int standardLower = 7;
    private static final int standardUpper = 21;

    public static String[] randomStrings(int number) {
        return randomStrings(number, standardLower, standardUpper);
    }

    public static String[] randomStrings(int number, int lowerLen, int upperLen) {
        String[] ret = new String[number];
        for (int i = 0; i < number; i++)
            ret[i] = randomString(lowerLen, upperLen);
        return ret;
    }

    public static String randomString() {
        return randomString(standardLower, standardUpper);
    }

    public static String randomString(int lowerLen, int upperLen) {
        Preconditions.checkState(lowerLen > 0 && upperLen >= lowerLen);
        int length = randomInt(lowerLen, upperLen);
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < length; i++) {
            s.append((char) randomInt(97, 120));
        }
        return s.toString();
    }

    /**
     * Generate a pseudorandom number using Math.random().
     *
     * @param lower minimum returned random number, inclusive
     * @param upper maximum returned random number, exclusive
     * @return the generated pseudorandom
     */
    public static int randomInt(int lower, int upper) {
        Preconditions.checkState(upper > lower);
        int interval = upper - lower;
        // Generate a random int on [lower, upper)
        double rand = Math.floor(Math.random() * interval) + lower;
        // Shouldn't happen
        if (rand >= upper)
            rand = upper - 1;
        // Cast and return
        return (int) rand;
    }

    /**
     * Generate a pseudorandom number using Math.random().
     *
     * @param lower minimum returned random number, inclusive
     * @param upper maximum returned random number, exclusive
     * @return the generated pseudorandom
     */
    public static long randomLong(long lower, long upper) {
        Preconditions.checkState(upper > lower);
        long interval = upper - lower;
        // Generate a random int on [lower, upper)
        double rand = Math.floor(Math.random() * interval) + lower;
        // Shouldn't happen
        if (rand >= upper)
            rand = upper - 1;
        // Cast and return
        return (long) rand;
    }

    @Test
    public void testRandomInt() {
        long sum = 0;
        int trials = 100000;
        for (int i = 0; i < trials; i++) {
            sum += randomInt(1, 101);
        }
        double avg = sum * 1.0 / trials;
        double error = (5 / Math.pow(trials, 0.3));
        //log.debug(error);
        assertTrue(Math.abs(avg - 50.5) < error);
    }

    @Test
    public void testRandomLong() {
        long sum = 0;
        int trials = 100000;
        for (int i = 0; i < trials; i++) {
            sum += randomLong(1, 101);
        }
        double avg = sum * 1.0 / trials;
        double error = (5 / Math.pow(trials, 0.3));
        //log.debug(error);
        assertEquals(50.5,avg,error);
    }

    @Test
    public void testRandomString() {
        for (int i = 0; i < 20; i++)
            log.debug(randomString(5, 20));
    }

}

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

package org.janusgraph.util.system;

/**
 * Utility class for dealing with {@link Thread}
 */
public class Threads {

    public static boolean oneAlive(Thread[] threads) {
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) return true;
        }
        return false;
    }

    public static void terminate(Thread[] threads) {
        for (Thread thread : threads) {
            if (thread != null && thread.isAlive()) thread.interrupt();
        }
    }

    public static final int DEFAULT_SLEEP_INTERVAL_MS = 100;

    public static boolean waitForCompletion(Thread[] threads) {
        return waitForCompletion(threads,Integer.MAX_VALUE);
    }

    public static boolean waitForCompletion(Thread[] threads, int maxWaitMillis) {
        return waitForCompletion(threads,maxWaitMillis,DEFAULT_SLEEP_INTERVAL_MS);
    }

    public static boolean waitForCompletion(Thread[] threads, int maxWaitMillis, int sleepPeriodMillis) {
        long endTime = System.currentTimeMillis()+maxWaitMillis;
        while (oneAlive(threads)) {
            long currentTime = System.currentTimeMillis();
            if (currentTime>=endTime) return false;
            try {
                Thread.sleep(Math.min(sleepPeriodMillis,endTime-currentTime));
            } catch (InterruptedException e) {
                System.err.println("Interrupted while waiting for completion of threads!");
            }
        }
        return true;
    }

}

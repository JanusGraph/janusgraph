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

public class MemoryAssess {

    private long memStart = 0;

    public MemoryAssess() {
    }

    public MemoryAssess start() {
        memStart = getMemoryUse();
        return this;
    }

    public long end() {
        return getMemoryUse() - memStart;
    }

    private static final long fSLEEP_INTERVAL = 100;

    public static long getMemoryUse() {
        putOutTheGarbage();
        putOutTheGarbage();
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        return (totalMemory - freeMemory);
    }

    private static void putOutTheGarbage() {
        collectGarbage();
        collectGarbage();
    }

    private static void collectGarbage() {
        try {
            System.gc();
            Thread.sleep(fSLEEP_INTERVAL);
            System.runFinalization();
            Thread.sleep(fSLEEP_INTERVAL);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }
    }
}

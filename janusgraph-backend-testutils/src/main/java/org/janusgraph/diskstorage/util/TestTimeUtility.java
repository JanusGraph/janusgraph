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

package org.janusgraph.diskstorage.util;


import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestTimeUtility {


    @Test
    public void testTimeSequence() throws Exception {
        Random r = new Random();
        Instant[] times = new Instant[10];
        for (int i = 0; i < times.length; i++) {
            times[i] = TimestampProviders.NANO.getTime();
            if (i > 0) assertTrue(times[i].compareTo(times[i - 1])>0, times[i] + " > " + times[i - 1]);
            Thread.sleep(r.nextInt(50) + 2);
        }
    }

}

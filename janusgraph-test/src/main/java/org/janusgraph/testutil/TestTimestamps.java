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

import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by bryn on 01/05/15.
 */
public class TestTimestamps {

    @Test
    public void testMicro() {
        testRoundTrip(TimestampProviders.MICRO);
        assertEquals(Instant.ofEpochSecond(1000), TimestampProviders.MICRO.getTime(1000000000));

    }

    @Test
    public void testMilli() {
        testRoundTrip(TimestampProviders.MILLI);
    }

    @Test
    public void testNano() {
        testRoundTrip(TimestampProviders.NANO);
    }

    private void testRoundTrip(TimestampProvider p) {
        Instant now = p.getTime();
        long time = p.getTime(now);
        Instant now2 = p.getTime(time);
        assertEquals(now, now2);
    }


}

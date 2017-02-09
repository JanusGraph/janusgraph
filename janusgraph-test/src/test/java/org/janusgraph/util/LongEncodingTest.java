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

package org.janusgraph.util;

import com.google.common.collect.Sets;
import org.janusgraph.util.encoding.LongEncoding;
import org.junit.Test;

import java.util.Random;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LongEncodingTest {


    @Test
    public void testEncoding() {
        final int number = 1000000;
        final Random r = new Random();
        long start = System.currentTimeMillis();
        for (int i=0;i<number;i++) {
            long l = Math.abs(r.nextLong());
            if (l==Long.MIN_VALUE) continue;
            assertEquals(l, LongEncoding.decode(LongEncoding.encode(l)));
        }
        System.out.println("Time to de/encode "+number+" longs (in ms): " + (System.currentTimeMillis()-start));
    }

    @Test
    public void testCaseInsensitivity() {
        Set<String> codes = Sets.newHashSet();
        for (int i = 0; i < 500000; i++) {
            assertTrue(codes.add(LongEncoding.encode(i).toLowerCase()));
        }
    }



}

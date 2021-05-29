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

import com.google.common.collect.ImmutableList;
import org.janusgraph.util.encoding.StringEncoding;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StringEncodingTest {

    @Test
    public void testAsciiStringEncoding() {
        String[] str = {"asdf3","","f232rdfjdhjkhfafb-38`138","8947(*&#$80124n"};
        for (String s : str) {
            assertTrue(StringEncoding.isAsciiString(s));
            assertEquals(Math.max(1,s.length()), StringEncoding.getAsciiByteLength(s));
            byte[] data = new byte[StringEncoding.getAsciiByteLength(s)];
            StringEncoding.writeAsciiString(data,0,s);
            assertEquals(s,StringEncoding.readAsciiString(data,0));
        }
        byte[] data = new byte[6];
        StringEncoding.writeAsciiString(data,0,"abc");
        StringEncoding.writeAsciiString(data,3,"xyz");
        assertEquals("abc", StringEncoding.readAsciiString(data, 0));
        assertEquals("xyz",StringEncoding.readAsciiString(data,3));
    }

    @Test
    public void testAsciiStringEncodingWithNull() {
        assertThrows(NullPointerException.class, () -> {
            StringEncoding.getAsciiByteLength(null);
        });
    }

    @Test
    public void testAsciiStringEncodingWithNonAscii() {
        assertThrows(IllegalArgumentException.class, () -> {
            StringEncoding.getAsciiByteLength("ösdf30snü+p");
        });
    }

    @Test
    public void testStringLaunder() {
        ImmutableList.of("asdf3", "", "f232rdfjdhjkhfafb-38`138", "8947(*&#$80124n", " _+%", "ösdf30snü+p")
            .forEach(s -> assertEquals(s, StringEncoding.launder(s)));
    }

    @Test
    public void testStringLaunderWithNull() {
        assertThrows(NullPointerException.class, () -> {
            StringEncoding.launder(null);
        });
    }

}

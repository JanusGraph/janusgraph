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

package org.janusgraph.util.encoding;

import com.google.common.base.Preconditions;

/**
 * Utility class for encoding longs in strings based on:
 * See <a href="https://stackoverflow.com/questions/2938482/encode-decode-a-long-to-a-string-using-a-fixed-set-of-letters-in-java">stackoverflow</a>
 *
 * @author https://stackoverflow.com/users/276101/polygenelubricants
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LongEncoding {

    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyz";

    public static long decode(String s) {
        return decode(s,BASE_SYMBOLS);
    }

    public static String encode(long num) {
        return encode(num,BASE_SYMBOLS);
    }

    public static long decode(String s, String symbols) {
        final int B = symbols.length();
        long num = 0;
        for (char ch : s.toCharArray()) {
            num *= B;
            int pos = symbols.indexOf(ch);
            if (pos<0) throw new NumberFormatException("Symbol set does not match string");
            num += pos;
        }
        return num;
    }

    public static String encode(long num, String symbols) {
        Preconditions.checkArgument(num>=0,"Expected non-negative number: %d", num);
        final int B = symbols.length();
        StringBuilder sb = new StringBuilder();
        while (num != 0) {
            sb.append(symbols.charAt((int) (num % B)));
            num /= B;
        }
        return sb.reverse().toString();
    }

}

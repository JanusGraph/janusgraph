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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StringEncoding {

    public static final String UTF8_ENCODING = "UTF-8";

    public static final Charset UTF8_CHARSET = StandardCharsets.UTF_8;

    public static boolean isAsciiString(String input) {
        Preconditions.checkNotNull(input);
        for (int i = 0; i < input.length(); i++) {
            int c = input.charAt(i);
            if (c>127 || c<=0) return false;
        }
        return true;
    }

    //Similar to {@link StringSerializer}

    public static int writeAsciiString(byte[] array, int startPos, String attribute) {
        Preconditions.checkArgument(isAsciiString(attribute));
        if (attribute.length()==0) {
            array[startPos++] = (byte)0x80;
        } else {
            for (int i = 0; i < attribute.length(); i++) {
                int c = attribute.charAt(i);
                assert c <= 127;
                byte b = (byte)c;
                if (i+1==attribute.length()) b |= 0x80; //End marker
                array[startPos++]=b;
            }
        }
        return startPos;
    }

    public static String readAsciiString(byte[] array, int startPos) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = 0xFF & array[startPos++];
            if (c!=0x80) sb.append((char)(c & 0x7F));
            if ((c & 0x80) > 0) break;
        }
        return sb.toString();
    }

    public static int getAsciiByteLength(String attribute) {
        Preconditions.checkArgument(isAsciiString(attribute));
        return attribute.isEmpty()?1:attribute.length();
    }

    public static String launder(String input) {
        Preconditions.checkNotNull(input);
        final StringBuilder sb = new StringBuilder();
        input.chars().forEach(c -> sb.append((char) Integer.valueOf(c).intValue()));
        return sb.toString();
    }

}

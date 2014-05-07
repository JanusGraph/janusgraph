
package com.thinkaurelius.titan.util.encoding;

import com.google.common.base.Preconditions;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StringEncoding {

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

    public static final int getAsciiByteLength(String attribute) {
        Preconditions.checkArgument(isAsciiString(attribute));
        return attribute.isEmpty()?1:attribute.length();
    }

}

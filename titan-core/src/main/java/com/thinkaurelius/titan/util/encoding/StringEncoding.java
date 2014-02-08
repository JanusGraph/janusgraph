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
            if (c>127) return false;
        }
        return true;
    }

}

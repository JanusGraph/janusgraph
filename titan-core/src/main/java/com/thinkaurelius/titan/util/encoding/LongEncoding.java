package com.thinkaurelius.titan.util.encoding;

import com.google.common.base.Preconditions;

/**
 * Utility class for encoding longs in strings based on:
 * {@linktourl http://stackoverflow.com/questions/2938482/encode-decode-a-long-to-a-string-using-a-fixed-set-of-letters-in-java}
 *
 * @author http://stackoverflow.com/users/276101/polygenelubricants
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LongEncoding {

    private static final String BASE_SYMBOLS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

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
            if (pos<0) throw new NumberFormatException("Sybmol set does not match string");
            num += pos;
        }
        return num;
    }

    public static String encode(long num, String symbols) {
        Preconditions.checkArgument(num>=0,"Expected non-negative number: " + num);
        final int B = symbols.length();
        StringBuilder sb = new StringBuilder();
        while (num != 0) {
            sb.append(symbols.charAt((int) (num % B)));
            num /= B;
        }
        return sb.reverse().toString();
    }

}

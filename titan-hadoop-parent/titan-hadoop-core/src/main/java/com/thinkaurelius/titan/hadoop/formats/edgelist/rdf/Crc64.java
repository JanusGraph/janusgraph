package com.thinkaurelius.titan.hadoop.formats.edgelist.rdf;

public class Crc64 {

    private static final long POLY64 = 0x42F0E1EBA9EA3693L;
    private static final long[] LOOKUPTABLE;

    static {
        LOOKUPTABLE = new long[0x100];
        for (int i = 0; i < 0x100; i++) {
            long crc = i;
            for (int j = 0; j < 8; j++) {
                if ((crc & 1) == 1) {
                    crc = (crc >>> 1) ^ POLY64;
                } else {
                    crc = (crc >>> 1);
                }
            }
            LOOKUPTABLE[i] = crc;
        }
    }

    public static long digest(final byte[] data) {
        long checksum = 0;

        for (int i = 0; i < data.length; i++) {
            final int lookupidx = ((int) checksum ^ data[i]) & 0xff;
            checksum = (checksum >>> 8) ^ LOOKUPTABLE[lookupidx];
        }

        return checksum;
    }
}
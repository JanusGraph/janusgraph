package com.thinkaurelius.titan.diskstorage.util;

public interface TimestampProvider {

    //    public long getApproxNSSinceEpoch(final boolean setLSB);
    public long getApproxNSSinceEpoch();

    public long sleepUntil(final long untilNS) throws InterruptedException;
}

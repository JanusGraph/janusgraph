package com.thinkaurelius.titan.diskstorage.util;

public interface TimestampProvider {
    
    public long getApproxNSSinceEpoch(final boolean setLSB);
}

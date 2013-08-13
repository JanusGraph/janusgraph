package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.locking.LockStatus;

import java.util.concurrent.TimeUnit;

/**
 * The timestamp and checked-ness of a held {@link ConsistentKeyLock}
 * 
 * {@see ConsistentKeyLockStore}
 */
public class ConsistentKeyLockStatus implements LockStatus {
    private long writeNS;
    private long expireNS;
    private boolean checked;

    public ConsistentKeyLockStatus(long writeTimestamp, TimeUnit writeUnits, long expireTimestamp, TimeUnit expireUnits) {
        this.writeNS =  TimeUnit.NANOSECONDS.convert(writeTimestamp, writeUnits);
        this.expireNS = TimeUnit.NANOSECONDS.convert(expireTimestamp,  expireUnits);
        this.checked = false;
    }

    @Override
    public long getExpirationTimestamp(TimeUnit tu) {
        return tu.convert(expireNS, TimeUnit.NANOSECONDS);
    }
    
    public long getWriteTimestamp(TimeUnit tu) {
        return tu.convert(writeNS, TimeUnit.NANOSECONDS);
    }

    public boolean isChecked() {
        return checked;
    }

    public void setChecked() {
        this.checked = true;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (checked ? 1231 : 1237);
        result = prime * result + (int) (expireNS ^ (expireNS >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ConsistentKeyLockStatus other = (ConsistentKeyLockStatus) obj;
        if (checked != other.checked)
            return false;
        if (expireNS != other.expireNS)
            return false;
        return true;
    }
}

package com.thinkaurelius.titan.diskstorage.locking.consistentkey;

import com.thinkaurelius.titan.diskstorage.util.time.Timepoint;
import com.thinkaurelius.titan.diskstorage.locking.LockStatus;

import java.util.concurrent.TimeUnit;

/**
 * The timestamps of a lock held by a {@link ConsistentKeyLocker}
 * and whether the held lock has or has not been checked.
 *
 * {@see ConsistentKeyLockStore}
 */
public class ConsistentKeyLockStatus implements LockStatus {

    private final Timepoint write;
    private final Timepoint expire;
    private boolean checked;

    public ConsistentKeyLockStatus(Timepoint written, Timepoint expire) {
        this.write = written;
        this.expire = expire;
        this.checked = false;
    }

    @Override
    public Timepoint getExpirationTimestamp() {
        return expire;
    }

    public long getWriteTimestamp(TimeUnit tu) {
        return write.getTimestamp(tu);
    }

    public Timepoint getWriteTimestamp() {
        return write;
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
        result = prime * result + ((expire == null) ? 0 : expire.hashCode());
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
        if (expire == null) {
            if (other.expire != null)
                return false;
        } else if (!expire.equals(other.expire))
            return false;
        return true;
    }
}

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

package org.janusgraph.diskstorage.locking.consistentkey;


import org.janusgraph.diskstorage.locking.LockStatus;

import java.time.Instant;

/**
 * The timestamps of a lock held by a {@link ConsistentKeyLocker}
 * and whether the held lock has or has not been checked.
 *
 */
public class ConsistentKeyLockStatus implements LockStatus {

    private final Instant write;
    private final Instant expire;
    private boolean checked;

    public ConsistentKeyLockStatus(Instant written, Instant expire) {
        this.write = written;
        this.expire = expire;
        this.checked = false;
    }

    @Override
    public Instant getExpirationTimestamp() {
        return expire;
    }


    public Instant getWriteTimestamp() {
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
            return other.expire == null;
        } else {
            return expire.equals(other.expire);
        }
    }
}

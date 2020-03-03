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

package org.janusgraph.diskstorage.locking;

import java.time.Instant;

/**
 * A single held lock's expiration time. This is used by {@link AbstractLocker}.
 *
 * @see AbstractLocker
 * @see org.janusgraph.diskstorage.locking.consistentkey.ConsistentKeyLockStatus
 */
public interface LockStatus {

    /**
     * Returns the moment at which this lock expires (inclusive).
     *
     * @return The expiration instant of this lock
     */
    Instant getExpirationTimestamp();
}

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

import org.janusgraph.diskstorage.PermanentBackendException;

/**
 * This exception signifies a failure to lock based on durable state. For
 * example, another machine holds the lock we attempted to claim. These
 * exceptions typically will not go away on retries unless a machine modifies
 * the underlying lock state in some way.
 * <p>
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class PermanentLockingException extends PermanentBackendException {

    private static final long serialVersionUID = 482890657293484420L;

    /**
     * @param msg Exception message
     */
    public PermanentLockingException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public PermanentLockingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public PermanentLockingException(Throwable cause) {
        this("Permanent locking failure", cause);
    }


}

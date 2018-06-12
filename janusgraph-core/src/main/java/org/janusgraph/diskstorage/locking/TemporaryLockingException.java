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

import org.janusgraph.diskstorage.TemporaryBackendException;

/**
 * This exception signifies a (potentially) temporary exception while attempting
 * to acquire a lock in the JanusGraph storage backend. These can occur due to
 * request timeouts, network failures, etc. Temporary failures represented by
 * this exception might disappear if the request is retried, even if no machine
 * modifies the underlying lock state between the failure and follow-up request.
 * <p>
 * 
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TemporaryLockingException extends TemporaryBackendException {

    private static final long serialVersionUID = 482890657293484420L;

    /**
     * @param msg Exception message
     */
    public TemporaryLockingException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public TemporaryLockingException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public TemporaryLockingException(Throwable cause) {
        this("Temporary locking failure", cause);
    }


}

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

package org.janusgraph.diskstorage;

/**
 * This exception signifies a (potentially) temporary exception in a JanusGraph storage backend,
 * that is, an exception that is due to a temporary unavailability or other exception that
 * is not permanent in nature.
 * <p>
 * If this exception is thrown it indicates that retrying the same operation might potentially
 * lead to success (but not necessarily)
 * <p>
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TemporaryBackendException extends BackendException {

    private static final long serialVersionUID = 9286719478969781L;

    /**
     * @param msg Exception message
     */
    public TemporaryBackendException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public TemporaryBackendException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public TemporaryBackendException(Throwable cause) {
        this("Temporary failure in storage backend", cause);
    }


}

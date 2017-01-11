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

package org.janusgraph.core;

/**
 * Exception thrown when a user defined query (e.g. a {@link JanusGraphVertex} or {@link JanusGraphQuery})
 * is invalid or could not be processed.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public class QueryException extends JanusGraphException {

    private static final long serialVersionUID = 1L;

    /**
     * @param msg Exception message
     */
    public QueryException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public QueryException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public QueryException(Throwable cause) {
        this("Exception in query.", cause);
    }

}

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


import org.janusgraph.util.datastructures.ExceptionUtil;

/**
 * Most general type of exception thrown by the JanusGraph graph database.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 */
public class JanusGraphException extends RuntimeException {

    private static final long serialVersionUID = 4056436257763972423L;

    /**
     * @param msg Exception message
     */
    public JanusGraphException(String msg) {
        super(msg);
    }

    /**
     * @param msg   Exception message
     * @param cause Cause of the exception
     */
    public JanusGraphException(String msg, Throwable cause) {
        super(msg, cause);
    }

    /**
     * Constructs an exception with a generic message
     *
     * @param cause Cause of the exception
     */
    public JanusGraphException(Throwable cause) {
        this("Exception in JanusGraph", cause);
    }

    /**
     * Checks whether this exception is cause by an exception of the given type.
     *
     * @param causeExceptionType exception type
     * @return true, if this exception is caused by the given type
     */
    public boolean isCausedBy(Class<?> causeExceptionType) {
        return ExceptionUtil.isCausedBy(this, causeExceptionType);
    }

}

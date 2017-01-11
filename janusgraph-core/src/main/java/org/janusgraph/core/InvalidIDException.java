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
 * JanusGraph represents element identifiers as longs, but not all numbers
 * in the representable space of longs are valid.  This exception can
 * be thrown when an invalid long ID is encountered.
 */
public class InvalidIDException extends JanusGraphException {

    public InvalidIDException(String msg) {
        super(msg);
    }

    public InvalidIDException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public InvalidIDException(Throwable cause) {
        super(cause);
    }
}

// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb

/**
 * This interface exists solely to avoid autoboxing primitive longs with Long.
 * 
 * A standard-library alternative to this interface is slated for JDK 8.
 * It currently exists in beta builds as java.util.stream.LongStream.LongIterator.
 */
interface LongIterator {
    long next()
    boolean hasNext()
}

/**
 * Returns a sequence of longs modulo some fixed limit and starting at some fixed value.
 */
class SequentialLongIterator implements LongIterator {
    
    private long i = 0L
    private long offset
    private long count
    private long max
    
    SequentialLongIterator(long count, long max, long offset) {
        this.count = count
        this.max = max
        this.offset = offset
    }
    
    long next() {
        (offset + i++) % max
    }
    
    boolean hasNext() {
        i < count
    }
}

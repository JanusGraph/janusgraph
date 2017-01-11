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

package org.janusgraph.diskstorage.keycolumnvalue;

/**
 * Consistency Levels for transactions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ConsistencyLevel {

    /**
     * The default consistency level afforded by the underlying storage backend
     */
    DEFAULT,

    /**
     * Consistency level which ensures that operations on a {@link KeyColumnValueStore} are key-consistent.
     */
    KEY_CONSISTENT,

    /**
     * Consistency level which ensures that operations on a {@link KeyColumnValueStore} are key-consistent with
     * respect to a local cluster where multiple local clusters form the entire (global) cluster.
     * In other words, {@link #KEY_CONSISTENT} ensures key consistency across the entire global cluster whereas this
     * is restricted to the local cluster.
     */
    LOCAL_KEY_CONSISTENT;


    public boolean isKeyConsistent() {
        switch (this) {
            case KEY_CONSISTENT:
            case LOCAL_KEY_CONSISTENT:
                return true;
            case DEFAULT:
                return false;
            default: throw new AssertionError(this.toString());
        }
    }

}

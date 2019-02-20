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

package org.janusgraph.diskstorage.util;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.StaticBuffer;

/**
 * Class representing a (key, column) pair.
 *
 * @author Dan LaRocque (dalaro@hopcount.org)
 */
public class KeyColumn {

    private final StaticBuffer key;
    private final StaticBuffer col;
    private int cachedHashCode;

    public KeyColumn(StaticBuffer key, StaticBuffer col) {
        this.key = Preconditions.checkNotNull(key);
        this.col = Preconditions.checkNotNull(col);
    }

    public StaticBuffer getKey() {
        return key;
    }

    public StaticBuffer getColumn() {
        return col;
    }

    @Override
    public int hashCode() {
        // if the hashcode is needed frequently, we should store it
        if (0 != cachedHashCode)
            return cachedHashCode;

        final int prime = 31;
        int result = 1;
        result = prime * result + col.hashCode();
        result = prime * result + key.hashCode();

        // This is only thread-safe because cachedHashCode is an int and not a long
        cachedHashCode = result;

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
        KeyColumn other = (KeyColumn) obj;
        return other.key.equals(key) && other.col.equals(col);
    }

    @Override
    public String toString() {
        return "KeyColumn [k=0x" + key.toString() +
                ", c=0x" + col.toString() + "]";
    }
}

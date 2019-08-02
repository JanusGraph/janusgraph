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

package org.janusgraph.graphdb.transaction.lock;

import com.google.common.base.Preconditions;

import java.util.Objects;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class LockTuple {

    private final Object[] elements;

    public LockTuple(Object... elements) {
        Preconditions.checkArgument(elements!=null && elements.length>0);
        for (Object o : elements) Preconditions.checkNotNull(o);
        this.elements=elements;
    }

    public int size() {
        return elements.length;
    }

    public Object get(int pos) {
        return elements[pos];
    }

    public Object[] getAll() {
        return elements;
    }

    @Override
    public int hashCode() {
        return Objects.hash(elements);
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !(oth instanceof LockTuple)) return false;
        LockTuple other = (LockTuple)oth;
        if (elements.length!=other.elements.length) return false;
        for (int i=0;i<elements.length;i++) if (!elements[i].equals(other.elements[i])) return false;
        return true;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[");
        for (int i = 0; i < elements.length; i++) {
            if (i>0) b.append(",");
            b.append(elements[i].toString());
        }
        b.append("]");
        return b.toString();
    }


}

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

import org.janusgraph.graphdb.types.CompositeIndexType;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexLockTuple extends LockTuple {

    private final CompositeIndexType index;

    public IndexLockTuple(CompositeIndexType index, Object... tuple) {
        super(tuple);
        this.index=index;
    }

    public CompositeIndexType getIndex() {
        return index;
    }

    @Override
    public int hashCode() {
        return super.hashCode()*10043 + Long.hashCode(index.getID());
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        else if (oth==null || !(oth instanceof IndexLockTuple)) return false;
        return super.equals(oth) && ((IndexLockTuple)oth).index.getID()==index.getID();
    }

    @Override
    public String toString() {
        return super.toString()+":"+index.getID();
    }


}

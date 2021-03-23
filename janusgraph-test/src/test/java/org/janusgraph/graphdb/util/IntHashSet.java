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

package org.janusgraph.graphdb.util;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.cursors.IntCursor;

import java.util.Iterator;

/**
 * Implementation of {@link IntSet} against {@link IntIntHashMap}.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IntHashSet extends IntIntHashMap implements IntSet {

    private static final long serialVersionUID = -7297353805905443841L;
    private static final int defaultValue = 1;

    public IntHashSet() {
        super();
    }

    public IntHashSet(int size) {
        super(size);
    }

    public boolean add(int value) {
        return super.put(value, defaultValue)==0;
    }

    public boolean addAll(int[] values) {
        boolean addedAll = true;
        for (int value : values) if (!add(value)) addedAll = false;
        return addedAll;
    }

    public boolean contains(int value) {
        return super.containsKey(value);
    }

    public int[] getAll() {
        KeysContainer keys = keys();
        int[] all = new int[keys.size()];
        Iterator<IntCursor> iterator = keys.iterator();
        int pos=0;
        while (iterator.hasNext()) all[pos++]=iterator.next().value;
        return all;
    }

    @Override
    public int hashCode() {
        return ArraysUtil.sum(getAll());
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!(other instanceof IntSet)) return false;
        IntSet oth = (IntSet) other;
        for (int value : values) {
            if (!oth.contains(value)) return false;
        }
        return size() == oth.size();
    }

}

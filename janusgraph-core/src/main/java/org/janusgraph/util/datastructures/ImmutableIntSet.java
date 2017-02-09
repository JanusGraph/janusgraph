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

package org.janusgraph.util.datastructures;

import com.google.common.base.Preconditions;

/**
 * Immutable set of integers
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ImmutableIntSet implements IntSet {

    private final int[] values;
    private final int hashcode;

    public ImmutableIntSet(int[] values) {
        Preconditions.checkNotNull(values);
        Preconditions.checkArgument(values.length > 0);
        this.values = values;
        hashcode = ArraysUtil.sum(values);
    }

    public ImmutableIntSet(int value) {
        this(new int[]{value});
    }

    @Override
    public boolean add(int value) {
        throw new UnsupportedOperationException("This IntSet is immutable");
    }

    @Override
    public boolean addAll(int[] values) {
        throw new UnsupportedOperationException("This IntSet is immutable");
    }

    @Override
    public boolean contains(int value) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == value) return true;
        }
        return false;
    }

    @Override
    public int[] getAll() {
        return values;
    }

    @Override
    public int size() {
        return values.length;
    }

    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!(other instanceof IntSet)) return false;
        IntSet oth = (IntSet) other;
        for (int i = 0; i < values.length; i++) {
            if (!oth.contains(values[i])) return false;
        }
        return size() == oth.size();
    }

}

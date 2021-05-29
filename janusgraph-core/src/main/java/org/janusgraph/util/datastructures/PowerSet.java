// Copyright 2020 JanusGraph Authors
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

import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class PowerSet<T> extends AbstractSet<Set<T>> {

    private final Set<T> originalSet;

    public PowerSet(final Set<T> originalSet) {
        this.originalSet = originalSet;
    }

    @Override
    public Iterator<Set<T>> iterator() {
        return new PowerSetIterator<>(originalSet);
    }

    @Override
    public int size() {
        return 1 << originalSet.size();
    }

    private static class PowerSetIterator<T> implements Iterator<Set<T>> {

        private final List<T> originalElements;
        private int position;
        private final int resultSize;

        public PowerSetIterator(final Set<T> originalSet) {
            if (originalSet == null) {
                throw new NullPointerException("Base set for a power set must not be null");
            }
            if (originalSet.size() > 30) {
                throw new IllegalArgumentException("Input set is too large, power set would exceed integer limit size");
            }

            // allow efficient access by index
            originalElements = new ArrayList<>(originalSet);
            resultSize = 1 << originalSet.size();
            position = 0;
        }

        @Override
        public boolean hasNext() {
            return position < resultSize;
        }

        @Override
        public Set<T> next() {
            Set<T> result = new HashSet<>(originalElements.size());
            for (int elementMap = position, elementIdx = 0; elementMap > 0; elementMap /= 2, ++elementIdx) {
                // we use elementMap as a binary representation of which elements are contained in the result set
                if (elementMap % 2 == 1) {
                    result.add(originalElements.get(elementIdx));
                }
            }
            ++position;
            return result;
        }
    }
}

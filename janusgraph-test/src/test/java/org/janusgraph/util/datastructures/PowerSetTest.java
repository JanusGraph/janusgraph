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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Florian Grieskamp (Florian.Grieskamp@gdata.de)
 */
public class PowerSetTest {

    @Test
    public void testEmptySet() {
        Set<Object> inputSet = new HashSet<Object>();

        Set<Set<Object>> powerSet = new HashSet<>();
        powerSet.add(new HashSet<>());

        assertTrue(equalIterators(powerSet.iterator(), new PowerSet(inputSet).iterator()));
    }

    @Test
    public void testSetWithOneElement() {
        Set<Object> inputSet = new HashSet<Object>();
        inputSet.add(1);

        Set<Set<Object>> powerSet = new HashSet<>();

        Set<Object> empty = new HashSet<>();
        powerSet.add(empty);

        Set<Object> oneOnly = new HashSet<>();
        oneOnly.add(1);
        powerSet.add(oneOnly);

        assertTrue(equalIterators(powerSet.iterator(), new PowerSet(inputSet).iterator()));
    }

    @Test
    public void testSetWithTwoElements() {
        Set<Object> inputSet = new HashSet<Object>();
        inputSet.add(1);
        inputSet.add(2);

        Set<Set<Object>> powerSet = new HashSet<>();

        Set<Object> empty = new HashSet<>();
        powerSet.add(empty);

        Set<Object> oneOnly = new HashSet<>();
        oneOnly.add(1);
        powerSet.add(oneOnly);

        Set<Object> twoOnly = new HashSet<>();
        twoOnly.add(2);
        powerSet.add(twoOnly);

        Set<Object> oneAndTwo = new HashSet<>();
        oneAndTwo.add(1);
        oneAndTwo.add(2);
        powerSet.add(oneAndTwo);

        assertTrue(equalIterators(powerSet.iterator(), new PowerSet(inputSet).iterator()));
    }

    private <T> boolean equalIterators(Iterator<T> a, Iterator<T> b) {
        while (a.hasNext() && b.hasNext()) {
            T aElement = a.next();
            T bElement = b.next();

            if ((aElement == null && bElement == null) || (aElement != null && aElement.equals(bElement))) {
                continue;
            }

            return false;
        }

        if (a.hasNext() || b.hasNext()) {
            return false;
        }

        return true;
    }

}

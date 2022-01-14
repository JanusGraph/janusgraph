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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Utility class for merging and sorting lists of ids
 * An id can either be a String or a number
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AbstractIdListUtil {

    private static void validateId(Object id) {
        if (!(id instanceof Number || id instanceof String)) {
            throw new IllegalArgumentException("id must be number or string, but get: " + id);
        }
    }

    public static int compare(Object id1, Object id2) {
        validateId(id1);
        validateId(id2);
        if (id1 instanceof Number && id2 instanceof String) return -1;
        if (id1 instanceof String && id2 instanceof Number) return 1;
        if (id1 instanceof String) {
            assert id2 instanceof String;
            return ((String) id1).compareTo((String) id2);
        } else {
            assert id1 instanceof Number;
            assert id2 instanceof Number;
            return Double.compare(((Number) id1).doubleValue(), ((Number) id2).doubleValue());
        }
    }


    public static boolean isSorted(List<Object> l, final boolean unique) {
        for (int i = 1; i < l.size(); i++) {
            if (compare(l.get(i), l.get(i - 1)) < 0 || (unique && Objects.equals(l.get(i), l.get(i - 1)))) return false;
        }
        return true;
    }

    public static boolean isSorted(List<Object> l) {
        return isSorted(l, false);
    }

    public static List<Object> mergeSort(List<Object> a, List<Object> b) {
        int positionA=0, positionB=0;
        List<Object> result = new ArrayList<>(a.size()+b.size());
        while (positionA<a.size() || positionB<b.size()) {
            Object next;
            if (positionA>=a.size()) {
                next=b.get(positionB++);
            } else if (positionB>=b.size()) {
                next=a.get(positionA++);
            } else if (compare(a.get(positionA), b.get(positionB)) <= 0) {
                next=a.get(positionA++);
            } else {
                next=b.get(positionB++);
            }
            Preconditions.checkArgument(result.isEmpty() || compare(result.get(result.size()-1), next) <= 0,
                    "The input lists are not sorted");
            result.add(next);
        }
        return result;
    }

    public static List<Object> mergeJoin(List<Object> a, List<Object> b, final boolean unique) {
        assert isSorted(a) : a.toString();
        assert isSorted(b) : b.toString();
        int counterA = 0, counterB = 0;
        int sizeA = a.size();
        int sizeB = b.size();
        List<Object> merge = new ArrayList<>(Math.min(sizeA, sizeB));
        int resultSize = 0;
        while (counterA < sizeA && counterB < sizeB) {
            if (Objects.equals(a.get(counterA), b.get(counterB))) {
                Object value = a.get(counterA);
                if (!unique) {
                    merge.add(value);
                    resultSize++;
                } else {
                    if (resultSize <= 0 || merge.get(resultSize - 1) != value) {
                        merge.add(value);
                        resultSize++;
                    }
                }
                counterA++;
                counterB++;
            } else if (compare(a.get(counterA), b.get(counterB)) < 0) {
                counterA++;
            } else {
                counterB++;
            }
        }
        return merge;
    }
}

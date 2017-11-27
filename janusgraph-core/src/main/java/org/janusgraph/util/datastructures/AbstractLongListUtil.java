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

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.Preconditions;

/**
 * Utility class for merging and sorting lists of longs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AbstractLongListUtil {


    public static boolean isSorted(LongArrayList l, final boolean unique) {
        for (int i = 1; i < l.size(); i++) {
            if (l.get(i) < l.get(i - 1) || (unique && l.get(i) == l.get(i - 1))) return false;
        }
        return true;
    }

    public static boolean isSorted(LongArrayList l) {
        return isSorted(l, false);
    }

    public static LongArrayList mergeSort(LongArrayList a, LongArrayList b) {
        int positionA=0, positionB=0;
        LongArrayList result = new LongArrayList(a.size()+b.size());
        while (positionA<a.size() || positionB<b.size()) {
            long next;
            if (positionA>=a.size()) {
                next=b.get(positionB++);
            } else if (positionB>=b.size()) {
                next=a.get(positionA++);
            } else if (a.get(positionA)<=b.get(positionB)) {
                next=a.get(positionA++);
            } else {
                next=b.get(positionB++);
            }
            Preconditions.checkArgument(result.isEmpty() || result.get(result.size()-1)<=next,
                    "The input lists are not sorted");
            result.add(next);
        }
        return result;
    }

    public static LongArrayList mergeJoin(LongArrayList a, LongArrayList b, final boolean unique) {
        assert isSorted(a) : a.toString();
        assert isSorted(b) : b.toString();
        int counterA = 0, counterB = 0;
        int sizeA = a.size();
        int sizeB = b.size();
        LongArrayList merge = new LongArrayList(Math.min(sizeA, sizeB));
        int resultSize = 0;
        while (counterA < sizeA && counterB < sizeB) {
            if (a.get(counterA) == b.get(counterB)) {
                long value = a.get(counterA);
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
            } else if (a.get(counterA) < b.get(counterB)) {
                counterA++;
            } else {
                assert a.get(counterA) > b.get(counterB);
                counterB++;
            }
        }
        return merge;
    }

    public static LongArrayList singleton(long el) {
        LongArrayList l = new LongArrayList(1);
        l.add(el);
        return l;
    }

}

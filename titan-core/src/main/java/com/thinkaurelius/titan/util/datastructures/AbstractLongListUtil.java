package com.thinkaurelius.titan.util.datastructures;

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
        int posa=0, posb=0;
        LongArrayList result = new LongArrayList(a.size()+b.size());
        while (posa<a.size() || posb<b.size()) {
            long next;
            if (posa>=a.size()) {
                next=b.get(posb++);
            } else if (posb>=b.size()) {
                next=a.get(posa++);
            } else if (a.get(posa)<=b.get(posb)) {
                next=a.get(posa++);
            } else {
                next=b.get(posb++);
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

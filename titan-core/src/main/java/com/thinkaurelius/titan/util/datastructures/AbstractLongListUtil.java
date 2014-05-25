package com.thinkaurelius.titan.util.datastructures;

import cern.colt.list.AbstractIntList;
import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;
import com.google.common.base.Preconditions;

/**
 * Utility class for merging and sorting lists of longs
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class AbstractLongListUtil {


    public static boolean isSorted(AbstractLongList l, final boolean unique) {
        long[] values = l.elements();
        for (int i = 1; i < l.size(); i++) {
            if (values[i] < values[i - 1] || (unique && values[i] == values[i - 1])) return false;
        }
        return true;
    }

    public static boolean isSorted(AbstractLongList l) {
        return isSorted(l, false);
    }

    public static AbstractLongList mergeSort(AbstractLongList a, AbstractLongList b) {
        int posa=0, posb=0;
        AbstractLongList result = new LongArrayList(a.size()+b.size());
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

    public static AbstractLongList mergeJoin(AbstractLongList a, AbstractLongList b, final boolean unique) {
        assert isSorted(a) : a.toString();
        assert isSorted(b) : b.toString();
        int counterA = 0, counterB = 0;
        int sizeA = a.size();
        int sizeB = b.size();
        long[] valuesA = a.elements();
        long[] valuesB = b.elements();
        LongArrayList merge = new LongArrayList(Math.min(sizeA, sizeB));
        int resultSize = 0;
        while (counterA < sizeA && counterB < sizeB) {
            if (valuesA[counterA] == valuesB[counterB]) {
                long value = valuesA[counterA];
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
            } else if (valuesA[counterA] < valuesB[counterB]) {
                counterA++;
            } else {
                assert valuesA[counterA] > valuesB[counterB];
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

package com.thinkaurelius.titan.util.datastructures;

import cern.colt.list.AbstractIntList;
import cern.colt.list.AbstractLongList;
import cern.colt.list.LongArrayList;

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

    public static boolean isSorted(AbstractIntList l, final boolean unique) {
        int[] values = l.elements();
        for (int i = 1; i < l.size(); i++) {
            if (values[i] < values[i - 1] || (unique && values[i] == values[i - 1])) return false;
        }
        return true;
    }

    public static boolean isSorted(AbstractIntList l) {
        return isSorted(l, false);
    }

    public static LongArrayList mergeJoin(AbstractLongList a, AbstractLongList b, final boolean unique) {
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

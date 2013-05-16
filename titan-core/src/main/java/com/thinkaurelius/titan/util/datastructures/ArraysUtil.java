package com.thinkaurelius.titan.util.datastructures;

import cern.colt.Arrays;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

/**
 * Utility class for sorting and retrieving from primitive arrays
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ArraysUtil {

    public static final boolean isSortedInc(long[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (i > 0 && arr[i] <= arr[i - 1]) return false;
        }
        return true;
    }

    public static final boolean isSortedInc(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (i > 0 && arr[i] <= arr[i - 1]) return false;
        }
        return true;
    }

    public static final long[] insertSortedInc(long[] arr, long element) {
        assert arr == null || isSortedInc(arr);
        long[] newarr = new long[(arr != null ? arr.length + 1 : 1)];
        int offset = 0;
        if (arr != null) {
            for (int i = 0; i < arr.length; i++) {
                Preconditions.checkArgument(element != arr[i]);
                if (element < arr[i]) {
                    newarr[i] = element;
                    offset = 1;
                }
                newarr[i + offset] = arr[i];
            }
        }
        if (offset == 0) newarr[newarr.length - 1] = element;
        return newarr;
    }

    public static final long[] arrayDifference(long[] arr, long[] subset) {
        long[] res = new long[arr.length - subset.length];
        int pos = 0;
        for (int i = 0; i < arr.length; i++) {
            if (!Longs.contains(subset, arr[i])) {
                res[pos] = arr[i];
                pos++;
            }
        }
        assert pos == res.length;
        return res;
    }

    public static final long[] mergeSortedInc(long[] a, long[] b) {
        assert isSortedInc(a) && isSortedInc(b);
        long[] res = new long[a.length + b.length];
        int ai = 0, bi = 0;
        while (ai < a.length || bi < b.length) {
            if (a[ai] < b[bi]) {
                res[ai + bi] = a[ai];
                ai++;
            } else if (b[bi] < a[ai]) {
                res[ai + bi] = b[bi];
                bi++;
            } else throw new IllegalArgumentException(Arrays.toString(a) + "|" + Arrays.toString(b));
        }
        return res;
    }

    public static final int sum(int[] values) {
        int sum = 0;
        for (int i = 0; i < values.length; i++) {
            sum += values[i];
        }
        return sum;
    }

    public static final int indexOfMin(double[] values) {
        if (values.length < 1) return -1;
        int index = 0;
        for (int i = 1; i < values.length; i++) {
            if (values[i] < values[index]) {
                index = i;
            }
        }
        return index;
    }

    public static final int indexOf(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) return i;
        }
        return -1;
    }

}

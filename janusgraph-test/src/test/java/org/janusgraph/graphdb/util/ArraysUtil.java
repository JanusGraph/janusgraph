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

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;

import java.util.Arrays;

/**
 * Utility class for sorting and retrieving from primitive arrays
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ArraysUtil {

    public static boolean isSortedInc(long[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (i > 0 && arr[i] <= arr[i - 1]) return false;
        }
        return true;
    }

    public static boolean isSortedInc(int[] arr) {
        for (int i = 0; i < arr.length; i++) {
            if (i > 0 && arr[i] <= arr[i - 1]) return false;
        }
        return true;
    }

    public static long[] insertSortedInc(long[] arr, long element) {
        assert arr == null || isSortedInc(arr);
        long[] newArray = new long[(arr != null ? arr.length + 1 : 1)];
        int offset = 0;
        if (arr != null) {
            for (int i = 0; i < arr.length; i++) {
                Preconditions.checkArgument(element != arr[i], "Element: %d equals to arr[%d]: %d", element, i, arr[i]);
                if (element < arr[i]) {
                    newArray[i] = element;
                    offset = 1;
                }
                newArray[i + offset] = arr[i];
            }
        }
        if (offset == 0) newArray[newArray.length - 1] = element;
        return newArray;
    }

    public static long[] arrayDifference(long[] arr, long[] subset) {
        long[] res = new long[arr.length - subset.length];
        int pos = 0;
        for (long anArr : arr) {
            if (!Longs.contains(subset, anArr)) {
                res[pos] = anArr;
                pos++;
            }
        }
        assert pos == res.length;
        return res;
    }

    public static long[] mergeSortedInc(long[] a, long[] b) {
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

    public static int sum(int[] values) {
        int sum = 0;
        for (int value : values) {
            sum += value;
        }
        return sum;
    }

    public static int indexOfMin(double[] values) {
        if (values.length < 1) return -1;
        int index = 0;
        for (int i = 1; i < values.length; i++) {
            if (values[i] < values[index]) {
                index = i;
            }
        }
        return index;
    }

    public static int indexOf(int[] array, int value) {
        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) return i;
        }
        return -1;
    }

}

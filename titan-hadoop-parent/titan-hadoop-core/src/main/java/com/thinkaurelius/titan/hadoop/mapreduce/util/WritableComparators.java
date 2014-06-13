package com.thinkaurelius.titan.hadoop.mapreduce.util;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WritableComparators {

    public static class DecreasingIntComparator extends IntWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        static {
            WritableComparator.define(DecreasingIntComparator.class, new DecreasingIntComparator());
        }
    }

    public static class DecreasingDoubleComparator extends DoubleWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        static {
            WritableComparator.define(DecreasingDoubleComparator.class, new DecreasingDoubleComparator());
        }
    }

    public static class DecreasingFloatComparator extends FloatWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        static {
            WritableComparator.define(DecreasingFloatComparator.class, new DecreasingFloatComparator());
        }
    }

    public static class DecreasingTextComparator extends Text.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        static {
            WritableComparator.define(DecreasingTextComparator.class, new DecreasingTextComparator());
        }
    }

    public static class DecreasingBooleanComparator extends BooleanWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }

        static {
            WritableComparator.define(DecreasingBooleanComparator.class, new DecreasingBooleanComparator());
        }
    }

}

package com.thinkaurelius.faunus;

import junit.framework.TestCase;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WeightedWritableTest extends TestCase {

    public void testComparable() {
        WeightedWritable<Text> a = new WeightedWritable<Text>(10, new Text("marko"));
        WeightedWritable<Text> b = new WeightedWritable<Text>(11, new Text("marko"));
        assertEquals(0, a.compareTo(b));

    }

    public void testSerialization() throws IOException {
        WeightedWritable<Text> a = new WeightedWritable<Text>(11, new Text("faunus"));
        assertEquals(a.get().toString(), "faunus");
        assertEquals(a.getWeight(), 11l);

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        a.write(new DataOutputStream(bytes));
        WeightedWritable<Text> a2 = new WeightedWritable<Text>(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(a, a2);
        assertEquals(a2.get().toString(), "faunus");
        assertEquals(a2.getWeight(), 11l);

        assertEquals(a.compareTo(a2), 0);
    }
}

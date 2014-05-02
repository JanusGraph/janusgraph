package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;

import junit.framework.TestCase;

import org.apache.hadoop.io.WritableComparator;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class HolderTest extends TestCase {

    public void testRawComparison() throws IOException {
        Holder<HadoopVertex> holder1 = new Holder<HadoopVertex>('a', new HadoopVertex(EmptyConfiguration.immutable(), 10));
        Holder<HadoopVertex> holder2 = new Holder<HadoopVertex>('b', new HadoopVertex(EmptyConfiguration.immutable(), 11));

        ByteArrayOutputStream bytes1 = new ByteArrayOutputStream();
        holder1.write(new DataOutputStream(bytes1));
        ByteArrayOutputStream bytes2 = new ByteArrayOutputStream();
        holder2.write(new DataOutputStream(bytes2));

        assertEquals(-1, WritableComparator.get(Holder.class).compare(bytes1.toByteArray(), 0, bytes1.size(), bytes2.toByteArray(), 0, bytes2.size()));
        assertEquals(1, WritableComparator.get(Holder.class).compare(bytes2.toByteArray(), 0, bytes2.size(), bytes1.toByteArray(), 0, bytes1.size()));
        assertEquals(0, WritableComparator.get(Holder.class).compare(bytes1.toByteArray(), 0, bytes1.size(), bytes1.toByteArray(), 0, bytes1.size()));
    }

    public void testSerialization1() throws IOException {
        HadoopVertex vertex = new HadoopVertex(EmptyConfiguration.immutable(), 1l);
        Holder<HadoopVertex> holder1 = new Holder<HadoopVertex>('a', vertex);
        assertEquals(holder1.get(), vertex);
        assertEquals(holder1.getTag(), 'a');

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();

        holder1.write(new DataOutputStream(bytes));
        Holder<HadoopVertex> holder2 = new Holder<HadoopVertex>(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(holder1, holder2);
        assertEquals(holder2.get(), vertex);
        assertEquals(holder2.getTag(), 'a');

        assertEquals(holder1.compareTo(holder2), 0);
    }

    /*public void testRawComparator() throws IOException {
        Holder.Comparator comparator = new Holder.Comparator();
        comparator.compare()
    }*/
}

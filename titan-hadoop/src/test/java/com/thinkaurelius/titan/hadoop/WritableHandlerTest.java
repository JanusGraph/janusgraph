package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.hadoop.mapreduce.util.WritableHandler;

import junit.framework.TestCase;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WritableHandlerTest extends TestCase {

    public void testTextTypeConversion() {
        WritableHandler handler = new WritableHandler(Text.class);
        Text text = (Text) handler.set("marko");
        assertEquals(text.toString(), "marko");
        text = (Text) handler.set(1.0d);
        assertEquals(text.toString(), "1.0");
        text = (Text) handler.set(1.0f);
        assertEquals(text.toString(), "1.0");
        text = (Text) handler.set(1l);
        assertEquals(text.toString(), "1");
        text = (Text) handler.set(1);
        assertEquals(text.toString(), "1");
        text = (Text) handler.set(true);
        assertEquals(text.toString(), "true");
    }

    public void testLongTypeConversion() {
        WritableHandler handler = new WritableHandler(LongWritable.class);
        LongWritable l = (LongWritable) handler.set("1");
        assertEquals(l.get(), 1l);
        l = (LongWritable) handler.set(1.0d);
        assertEquals(l.get(), 1l);
        l = (LongWritable) handler.set(1.0f);
        assertEquals(l.get(), 1l);
        l = (LongWritable) handler.set(1l);
        assertEquals(l.get(), 1l);
        l = (LongWritable) handler.set(1);
        assertEquals(l.get(), 1l);
        try {
            l = (LongWritable) handler.set(true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    public void testDoubleTypeConversion() {
        WritableHandler handler = new WritableHandler(DoubleWritable.class);
        DoubleWritable d = (DoubleWritable) handler.set("1.0");
        assertEquals(d.get(), 1.0d);
        d = (DoubleWritable) handler.set(1.0d);
        assertEquals(d.get(), 1.0d);
        d = (DoubleWritable) handler.set(1.0f);
        assertEquals(d.get(), 1.0d);
        d = (DoubleWritable) handler.set(1l);
        assertEquals(d.get(), 1.0d);
        d = (DoubleWritable) handler.set(1);
        assertEquals(d.get(), 1.0d);
        try {
            d = (DoubleWritable) handler.set(true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    public void testFloatTypeConversion() {
        WritableHandler handler = new WritableHandler(FloatWritable.class);
        FloatWritable f = (FloatWritable) handler.set("1");
        assertEquals(f.get(), 1.0f);
        f = (FloatWritable) handler.set(1.0d);
        assertEquals(f.get(), 1.0f);
        f = (FloatWritable) handler.set(1.0f);
        assertEquals(f.get(), 1.0f);
        f = (FloatWritable) handler.set(1l);
        assertEquals(f.get(), 1.0f);
        f = (FloatWritable) handler.set(1);
        assertEquals(f.get(), 1.0f);
        try {
            f = (FloatWritable) handler.set(true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }

    public void testIntTypeConversion() {
        WritableHandler handler = new WritableHandler(IntWritable.class);
        IntWritable i = (IntWritable) handler.set("1");
        assertEquals(i.get(), 1);
        i = (IntWritable) handler.set(1.0d);
        assertEquals(i.get(), 1);
        i = (IntWritable) handler.set(1.0f);
        assertEquals(i.get(), 1);
        i = (IntWritable) handler.set(1l);
        assertEquals(i.get(), 1);
        i = (IntWritable) handler.set(1);
        assertEquals(i.get(), 1);
        try {
            i = (IntWritable) handler.set(true);
            assertTrue(false);
        } catch (IllegalArgumentException e) {
            assertTrue(true);
        }
    }
}

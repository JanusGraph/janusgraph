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

package org.janusgraph.graphdb.serializer;


import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.attribute.StringSerializer;
import org.janusgraph.graphdb.serializer.attributes.TClass1;
import org.janusgraph.graphdb.serializer.attributes.TClass1Serializer;
import org.janusgraph.graphdb.serializer.attributes.TClass2;
import org.janusgraph.graphdb.serializer.attributes.TClass2Serializer;
import org.janusgraph.graphdb.serializer.attributes.TEnum;
import org.janusgraph.graphdb.serializer.attributes.TEnumSerializer;
import org.janusgraph.graphdb.serializer.attributes.THashMapSerializer;
import org.janusgraph.testutil.RandomGenerator;
import org.junit.jupiter.api.Test;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.context.SpatialContextFactory;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.io.BinaryCodec;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class SerializerTest extends SerializerTestCommon {

    private static final Logger log =
            LoggerFactory.getLogger(SerializerTest.class);

    @Test
    public void objectWriteReadTest() {
        serialize.registerClass(2,TClass1.class, new TClass1Serializer());
        serialize.registerClass(80342,TClass2.class, new TClass2Serializer());
        serialize.registerClass(999,TEnum.class, new TEnumSerializer());
        objectWriteRead();
    }

    @Test
    public void comparableStringSerialization() {
        //Characters
        DataOutput out = serialize.getDataOutput(((int) Character.MAX_VALUE) * 2 + 8);
        for (char c = Character.MIN_VALUE; c < Character.MAX_VALUE; c++) {
            out.writeObjectNotNull(c);
        }
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        for (char c = Character.MIN_VALUE; c < Character.MAX_VALUE; c++) {
            assertEquals(c, serialize.readObjectNotNull(b, Character.class).charValue());
        }


        //String
        for (int t = 0; t < 10000; t++) {
            DataOutput out1 = serialize.getDataOutput(32 + 5);
            DataOutput out2 = serialize.getDataOutput(32 + 5);
            String s1 = RandomGenerator.randomString(1, 32);
            String s2 = RandomGenerator.randomString(1, 32);
            out1.writeObjectByteOrder(s1,String.class);
            out2.writeObjectByteOrder(s2,String.class);
            StaticBuffer b1 = out1.getStaticBuffer();
            StaticBuffer b2 = out2.getStaticBuffer();
            assertEquals(s1, serialize.readObjectByteOrder(b1.asReadBuffer(), String.class));
            assertEquals(s2, serialize.readObjectByteOrder(b2.asReadBuffer(), String.class));
            assertEquals(Integer.signum(s1.compareTo(s2)), Integer.signum(b1.compareTo(b2)), s1 + " vs " + s2);
        }
    }

    @Test
    public void classSerialization() {
        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(Boolean.class);
        out.writeObjectNotNull(Byte.class);
        out.writeObjectNotNull(Double.class);
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(Boolean.class, serialize.readObjectNotNull(b, Class.class));
        assertEquals(Byte.class, serialize.readObjectNotNull(b, Class.class));
        assertEquals(Double.class, serialize.readObjectNotNull(b, Class.class));
    }

    @Test
    public void parallelDeserialization() throws InterruptedException {
        serialize.registerClass(1,TClass2.class, new TClass2Serializer());

        final long value = 8;
        final String str = "123456";
        final TClass2 c = new TClass2("abcdefg",333);

        DataOutput out = serialize.getDataOutput(128);
        out.putLong(value);
        out.writeClassAndObject(value);
        out.writeObject(c, TClass2.class);
        out.writeObjectNotNull(str);
        final StaticBuffer b = out.getStaticBuffer();

        int numThreads = 4;
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < 100000; j++) {
                    ReadBuffer buffer = b.asReadBuffer();
                    assertEquals(8, buffer.getLong());
                    assertEquals (value , (long)serialize.readClassAndObject(buffer));
                    assertEquals(c,serialize.readObject(buffer,TClass2.class));
                    assertEquals(str,serialize.readObjectNotNull(buffer,String.class));
                }
            });
            threads[i].start();
        }
        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
    }


    @Test
    public void primitiveSerialization() {
        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(Boolean.FALSE);
        out.writeObjectNotNull(Boolean.TRUE);
        out.writeObjectNotNull(Byte.MIN_VALUE);
        out.writeObjectNotNull(Byte.MAX_VALUE);
        out.writeObjectNotNull((byte) 0);
        out.writeObjectNotNull(Short.MIN_VALUE);
        out.writeObjectNotNull(Short.MAX_VALUE);
        out.writeObjectNotNull((short) 0);
        out.writeObjectNotNull(Character.MIN_VALUE);
        out.writeObjectNotNull(Character.MAX_VALUE);
        out.writeObjectNotNull('a');
        out.writeObjectNotNull(Integer.MIN_VALUE);
        out.writeObjectNotNull(Integer.MAX_VALUE);
        out.writeObjectNotNull(0);
        out.writeObjectNotNull(Long.MIN_VALUE);
        out.writeObjectNotNull(Long.MAX_VALUE);
        out.writeObjectNotNull(0L);
        out.writeObjectNotNull((float) 0.0);
        out.writeObjectNotNull(0.0);

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(Boolean.FALSE, serialize.readObjectNotNull(b, Boolean.class));
        assertEquals(Boolean.TRUE, serialize.readObjectNotNull(b, Boolean.class));
        assertEquals(Byte.MIN_VALUE, serialize.readObjectNotNull(b, Byte.class).longValue());
        assertEquals(Byte.MAX_VALUE, serialize.readObjectNotNull(b, Byte.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Byte.class).longValue());
        assertEquals(Short.MIN_VALUE, serialize.readObjectNotNull(b, Short.class).longValue());
        assertEquals(Short.MAX_VALUE, serialize.readObjectNotNull(b, Short.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Short.class).longValue());
        assertEquals(Character.MIN_VALUE, serialize.readObjectNotNull(b, Character.class).charValue());
        assertEquals(Character.MAX_VALUE, serialize.readObjectNotNull(b, Character.class).charValue());
        assertEquals(new Character('a'), serialize.readObjectNotNull(b, Character.class));
        assertEquals(Integer.MIN_VALUE, serialize.readObjectNotNull(b, Integer.class).longValue());
        assertEquals(Integer.MAX_VALUE, serialize.readObjectNotNull(b, Integer.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Integer.class).longValue());
        assertEquals(Long.MIN_VALUE, serialize.readObjectNotNull(b, Long.class).longValue());
        assertEquals(Long.MAX_VALUE, serialize.readObjectNotNull(b, Long.class).longValue());
        assertEquals(0, serialize.readObjectNotNull(b, Long.class).longValue());
        assertEquals(0.0, serialize.readObjectNotNull(b, Float.class), 1e-20);
        assertEquals(0.0, serialize.readObjectNotNull(b, Double.class), 1e-20);

    }


    @Test
    public void testObjectVerification() {
        serialize.registerClass(2,TClass1.class, new TClass1Serializer());
        TClass1 t1 = new TClass1(24223,0.25f);

        DataOutput out = serialize.getDataOutput(128);
        out.writeClassAndObject(t1);
        out.writeClassAndObject(null);
        out.writeObject(t1,TClass1.class);
        out.writeObject(null,TClass1.class);

        //Test failure
        for (Object o : new Object[]{new TClass2("abc",2),Calendar.getInstance(), Lists.newArrayList()}) {
            try {
                out.writeObjectNotNull(o);
                fail();
            } catch (Exception ignored) {

            }
        }

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(t1, serialize.readClassAndObject(b));
        assertNull(serialize.readClassAndObject(b));
        assertEquals(t1, serialize.readObject(b, TClass1.class));
        assertNull(serialize.readObject(b, TClass1.class));

        assertFalse(b.hasRemaining());
    }


    @Test
    public void longWriteTest() {
        multipleStringWrite();
    }

    @Test
    public void largeWriteTest() {
        final String base = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"; //26 chars
        final StringBuilder str = new StringBuilder();
        for (int i = 0; i < 100; i++) str.append(base);
        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(str.toString());
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        assertEquals(str.toString(), serialize.readObjectNotNull(b, String.class));
        assertFalse(b.hasRemaining());
    }

    @Test
    public void enumSerializeTest() {
        serialize.registerClass(1,TEnum.class, new TEnumSerializer());
        DataOutput out = serialize.getDataOutput(128);
        out.writeObjectNotNull(TEnum.TWO);
        out.writeObjectNotNull(TEnum.THREE);
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        assertEquals(TEnum.TWO, serialize.readObjectNotNull(b, TEnum.class));
        assertEquals(TEnum.THREE, serialize.readObjectNotNull(b, TEnum.class));
        assertFalse(b.hasRemaining());

    }
    
    @Test
    public void customHashMapSerializeTest() {
        serialize.registerClass(1,HashMap.class, new THashMapSerializer());
        DataOutput out = serialize.getDataOutput(128);

        final String property1 = "property1";
        final String value1 = "value1";
        final HashMap<String, Object> hashMapIn = new HashMap<>();
        hashMapIn.put(property1, value1);
        out.writeObjectNotNull(hashMapIn);

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        if (printStats) log.debug(bufferStats(b));
        
        final HashMap<String, Object> hashMapOut = serialize.readObjectNotNull(b, HashMap.class);
        assertNotNull(hashMapOut);
        assertEquals(2, hashMapOut.size());
        assertEquals(value1, hashMapOut.get(property1));
        assertTrue(hashMapOut.containsKey(THashMapSerializer.class.getName())); // THashMapSerializer adds this
    }

    private StaticBuffer getStringBuffer(String value) {
        DataOutput o = serialize.getDataOutput(value.length()+10);
        o.writeObject(value,String.class);
        return o.getStaticBuffer();
    }

    @Test
    public void testStringCompression() {
        //ASCII encoding
        for (int t = 0; t < 100; t++) {
            String x = getRandomString(StringSerializer.TEXT_COMPRESSION_THRESHOLD -1,ASCII_VALUE);
            assertEquals(x.length()+1, getStringBuffer(x).length());
        }

        //SMAZ Encoding
//        String[] texts = {
//                "To Sherlock Holmes she is always the woman. I have seldom heard him mention her under any other name. In his eyes she eclipses and predominates the whole of her sex.",
//                "His manner was not effusive. It seldom was; but he was glad, I think, to see me. With hardly a word spoken, but with a kindly eye, he waved me to an armchair",
//                "I could not help laughing at the ease with which he explained his process of deduction.",
//                "A man entered who could hardly have been less than six feet six inches in height, with the chest and limbs of a Hercules. His dress was rich with a richness which would, in England"
//        };
//        for (String text : texts) {
//            assertTrue(text.length()> StringSerializer.TEXT_COMPRESSION_THRESHOLD);
//            StaticBuffer s = getStringBuffer(text);
////            System.out.println(String.format("String length [%s] -> byte size [%s]",text.length(),s.length()));
//            assertTrue(text.length()>s.length()); //Test that actual compression is happening
//        }

        //Gzip Encoding
        String[] patterns = { "aQd>@!as/df5h", "sdfodoiwk", "sdf", "ab", "asdfwewefefwdfkajhqwkdhj"};
        int targetLength = StringSerializer.LONG_COMPRESSION_THRESHOLD*5;
        for (String pattern : patterns) {
            StringBuilder sb = new StringBuilder(targetLength);
            for (int i=0; i<targetLength/pattern.length(); i++) sb.append(pattern);
            String text = sb.toString();
            assertTrue(text.length()> StringSerializer.LONG_COMPRESSION_THRESHOLD);
            StaticBuffer s = getStringBuffer(text);
//            System.out.println(String.format("String length [%s] -> byte size [%s]",text.length(),s.length()));
            assertTrue(text.length()>s.length()*10); //Test that radical compression is happening
        }

        for (int t = 0; t < 10000; t++) {
            String x = STRING_FACTORY.newInstance();
            DataOutput o = serialize.getDataOutput(64);
            o.writeObject(x,String.class);
            ReadBuffer r = o.getStaticBuffer().asReadBuffer();
            String y = serialize.readObject(r, String.class);
            assertEquals(x,y);
        }

    }

    @Test
    public void testSerializationMixture() {
        serialize.registerClass(1,TClass1.class, new TClass1Serializer());

        for (int t = 0; t < 1000; t++) {
            DataOutput out = serialize.getDataOutput(128);
            int num = random.nextInt(100)+1;
            final List<SerialEntry> entries = new ArrayList<>(num);
            for (int i = 0; i < num; i++) {
                Map.Entry<Class,Factory> type = Iterables.get(TYPES.entrySet(),random.nextInt(TYPES.size()));
                Object element = type.getValue().newInstance();
                boolean notNull = true;
                if (random.nextDouble()<0.5) {
                    notNull = false;
                    if (random.nextDouble()<0.2) element=null;
                }
                entries.add(new SerialEntry(element,type.getKey(),notNull));
                if (notNull) out.writeObjectNotNull(element);
                else out.writeObject(element,type.getKey());
            }
            StaticBuffer sb = out.getStaticBuffer();
            ReadBuffer in = sb.asReadBuffer();
            for (SerialEntry entry : entries) {
                Object read;
                if (entry.notNull) read = serialize.readObjectNotNull(in,entry.clazz);
                else read = serialize.readObject(in,entry.clazz);
                if (entry.object==null) assertNull(read);
                else if (entry.clazz.isArray()) {
                    assertEquals(Array.getLength(entry.object),Array.getLength(read));
                    for (int i = 0; i < Array.getLength(read); i++) {
                        assertEquals(Array.get(entry.object,i),Array.get(read,i));
                    }
                } else assertEquals(entry.object,read);
            }
        }
    }

    @Test
    public void testSerializedOrder() {
        serialize.registerClass(1,TClass1.class, new TClass1Serializer());

        final Map<Class,Factory> sortTypes = new HashMap<>();
        for (Map.Entry<Class,Factory> entry : TYPES.entrySet()) {
            if (serialize.isOrderPreservingDatatype(entry.getKey()))
                sortTypes.put(entry.getKey(),entry.getValue());
        }
        assertEquals(10,sortTypes.size());
        for (int t = 0; t < 3000000; t++) {
            DataOutput o1 = serialize.getDataOutput(64);
            DataOutput o2 = serialize.getDataOutput(64);
            Map.Entry<Class,Factory> type = Iterables.get(sortTypes.entrySet(),random.nextInt(sortTypes.size()));
            Comparable c1 = (Comparable)type.getValue().newInstance();
            Comparable c2 = (Comparable)type.getValue().newInstance();
            o1.writeObjectByteOrder(c1,type.getKey());
            o2.writeObjectByteOrder(c2,type.getKey());
            StaticBuffer s1 = o1.getStaticBuffer();
            StaticBuffer s2 = o2.getStaticBuffer();
            assertEquals(Math.signum(c1.compareTo(c2)),Math.signum(s1.compareTo(s2)));
            Object c1o = serialize.readObjectByteOrder(s1.asReadBuffer(),type.getKey());
            Object c2o = serialize.readObjectByteOrder(s2.asReadBuffer(),type.getKey());
            assertEquals(c1,c1o);
            assertEquals(c2,c2o);
        }


    }

    @Test
    public void testLegacyPointSerialization() {
        Geoshape geo = Geoshape.point(0.5, 2.5);
        Geoshape geo2 = Geoshape.point(1.5, 3.5);
        DataOutput out = serialize.getDataOutput(128);

        int length = geo.size();
        VariableLong.writePositive(out,length);
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < length; j++) {
                Geoshape.Point point = geo.getPoint(j);
                out.putFloat((float) (i==0 ? geo.getPoint(j).getLatitude() : geo.getPoint(j).getLongitude()));
            }
        }
        
        // Add a second point to the same buffer
        
        length = geo2.size();
        VariableLong.writePositive(out,length);
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < length; j++) {
                Geoshape.Point point = geo2.getPoint(j);
                out.putFloat((float) (i==0 ? geo2.getPoint(j).getLatitude() : geo2.getPoint(j).getLongitude()));
            }
        }

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        
        assertEquals(geo, serialize.readObjectNotNull(b, Geoshape.class));
        assertEquals(geo2, serialize.readObjectNotNull(b, Geoshape.class));
    }

    @Test
    public void testLegacyNonJtsSerialization() throws Exception {
        final SpatialContextFactory factory = new SpatialContextFactory();
        factory.geo = true;
        final SpatialContext context = new SpatialContext(factory);
        BinaryCodec binaryCodec = new BinaryCodec(context, factory);

        Shape[] shapes = new Shape[] {
            context.getShapeFactory().pointXY(2.5, 0.5),
            context.getShapeFactory().rect(2.5, 3.5, 0.5, 1.5),
            context.getShapeFactory().circle(2.5, 0.5, DistanceUtils.dist2Degrees(5, DistanceUtils.EARTH_MEAN_RADIUS_KM))
        };

        DataOutput out = serialize.getDataOutput(128);
        for (final Shape shape : shapes) {
            // manually serialize with non-JTS codec
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            outputStream.write(1);
            try (DataOutputStream dataOutput = new DataOutputStream(outputStream)) {
                binaryCodec.writeShape(dataOutput, shape);
                dataOutput.flush();
            }
            outputStream.flush();
            byte[] bytes = outputStream.toByteArray();
            VariableLong.writePositive(out,bytes.length);
            out.putBytes(bytes);
        }

        // deserialize with standard serializer
        ReadBuffer b = out.getStaticBuffer().asReadBuffer();
        assertEquals(Geoshape.geoshape(shapes[0]), serialize.readObjectNotNull(b, Geoshape.class));
        assertEquals(Geoshape.geoshape(shapes[1]), serialize.readObjectNotNull(b, Geoshape.class));
        assertEquals(Geoshape.geoshape(shapes[2]), serialize.readObjectNotNull(b, Geoshape.class));
    }

    @Test
    public void jsonObjectSerialization() throws IOException {

        jsonSerialization(ObjectNode.class, "{\"key1\":\"test\",\"key2\":123}");
    }

    @Test
    public void jsonArraySerialization() throws IOException {

        jsonSerialization(ArrayNode.class, "[\"val1\",\"val2\",\"val3\"]");
    }

    private static class SerialEntry {

        final Object object;
        final Class clazz;
        final boolean notNull;


        private SerialEntry(Object object, Class clazz, boolean notNull) {
            this.object = object;
            this.clazz = clazz;
            this.notNull = notNull;
        }
    }


    public interface Factory<T> {

        T newInstance();

    }

    public static final Random random = new Random();

    public static final int MAX_CHAR_VALUE = 20000;
    public static final int ASCII_VALUE = 128;

    public static String getRandomString(int maxSize, int maxChar) {
        int charOffset = 10;
        int size = random.nextInt(maxSize);
        StringBuilder sb = new StringBuilder(size);

        for (int i = 0; i < size; i++) {
            sb.append((char)(random.nextInt(maxChar-charOffset)+charOffset));
        }
        return sb.toString();
    }


    public static final Factory<String> STRING_FACTORY = () -> {
        if (random.nextDouble()>0.1) {
            return getRandomString(StringSerializer.TEXT_COMPRESSION_THRESHOLD *2,
                    random.nextDouble()>0.5?ASCII_VALUE:MAX_CHAR_VALUE);
        } else {
            return getRandomString(StringSerializer.LONG_COMPRESSION_THRESHOLD*4,
                    random.nextDouble()>0.5?ASCII_VALUE:MAX_CHAR_VALUE);
        }
    };

    public static float randomGeoPoint() {
        return random.nextFloat()*180.0f-90.0f;
    }

    public static List<double[]> randomGeoPoints(int n) {
        List<double[]> points = new ArrayList<>();
        for (int i=0; i<n; i++) {
            points.add(new double[] {random.nextFloat()*360.0f-180.0f, random.nextFloat()*180.0f-90.0f});
        }
        return points;
    }

    public static final Map<Class,Factory> TYPES = new HashMap<Class,Factory>() {{
        put(Byte.class, (Factory<Byte>) () -> (byte)random.nextInt());
        put(Short.class, (Factory<Short>) () -> (short)random.nextInt());
        put(Integer.class, (Factory<Integer>) random::nextInt);
        put(Long.class, (Factory<Long>) random::nextLong);
        put(Boolean.class, (Factory<Boolean>) () -> random.nextInt(2)==0);
        put(Character.class, (Factory<Character>) () -> (char)random.nextInt());
        put(Date.class, (Factory<Date>) () -> new Date(random.nextLong()));
        put(Float.class, (Factory<Float>) () -> random.nextFloat()*10000 - 10000/2.0f);
        put(Double.class, (Factory<Double>) () -> random.nextDouble()*10000000 - 10000000/2.0);
        put(Geoshape.class, (Factory<Geoshape>) () -> {
            final double alpha = random.nextDouble();
            final double x0=randomGeoPoint(), y0=randomGeoPoint(), x1=randomGeoPoint(), y1=randomGeoPoint();
            if (alpha>0.75) {
                final double minx=Math.min(x0,x1), miny=Math.min(y0,y1);
                final double maxx=minx==x0? x1 : x0, maxy=miny==y0 ? y1 : y0;
                return Geoshape.box(miny, minx, maxy, maxx);
            } else if (alpha>0.5) {
                return Geoshape.circle(y0,x0,random.nextInt(100)+1);
            } else if (alpha>0.25) {
                return Geoshape.line(Arrays.asList(new double[][] {{x0,y0},{x0,y1},{x1,y1},{x1,y0}}));
            } else {
                return Geoshape.polygon(Arrays.asList(new double[][] {{x0,y0},{x1,y0},{x1,y1},{x0,y1},{x0,y0}}));
            }
        });
        put(String.class, STRING_FACTORY);
        put(boolean[].class,getArrayFactory(boolean.class,get(Boolean.class)));
        put(byte[].class,getArrayFactory(byte.class,get(Byte.class)));
        put(short[].class,getArrayFactory(short.class,get(Short.class)));
        put(int[].class,getArrayFactory(int.class,get(Integer.class)));
        put(long[].class,getArrayFactory(long.class,get(Long.class)));
        put(float[].class,getArrayFactory(float.class,get(Float.class)));
        put(double[].class,getArrayFactory(double.class,get(Double.class)));
        put(char[].class,getArrayFactory(char.class,get(Character.class)));
        put(String[].class,getArrayFactory(String.class,get(String.class)));
        put(TClass1.class, (Factory<TClass1>) () -> new TClass1(random.nextLong(),random.nextFloat()));
    }};

    private static Factory getArrayFactory(final Class ct, final Factory f) {
        return () -> {
            final int length = random.nextInt(100);
            final Object array = Array.newInstance(ct,length);
            for (int i = 0; i < length; i++) {
                if (ct==boolean.class) Array.setBoolean(array,i, (Boolean) f.newInstance());
                else if (ct==byte.class) Array.setByte(array,i, (Byte) f.newInstance());
                else if (ct==short.class) Array.setShort(array,i, (Short) f.newInstance());
                else if (ct==int.class) Array.setInt(array,i, (Integer) f.newInstance());
                else if (ct==long.class) Array.setLong(array,i, (Long) f.newInstance());
                else if (ct==float.class) Array.setFloat(array,i, (Float) f.newInstance());
                else if (ct==double.class) Array.setDouble(array,i, (Double) f.newInstance());
                else if (ct==char.class) Array.setChar(array,i, (Character) f.newInstance());
                else Array.set(array,i, f.newInstance());
            }
            return array;
        };
    }

    private <T extends JsonNode> void jsonSerialization(Class<T> type, String jsonContent) throws IOException {

        ObjectMapper objectMapper = new ObjectMapper();

        T jsonNode = type.cast(objectMapper.readTree(jsonContent));

        DataOutput out = serialize.getDataOutput(128);

        out.writeObjectNotNull(jsonNode);

        ReadBuffer b = out.getStaticBuffer().asReadBuffer();

        assertEquals(jsonNode, serialize.readObjectNotNull(b, type));
    }


    //Arrays (support null serialization)


}


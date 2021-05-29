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

package org.janusgraph.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.shaded.jackson.databind.node.ArrayNode;
import org.apache.tinkerpop.shaded.jackson.databind.node.ObjectNode;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.core.attribute.Geoshape;
import org.janusgraph.core.attribute.GeoshapeSerializer;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.WriteBuffer;
import org.janusgraph.diskstorage.idmanagement.ConflictAvoidanceMode;
import org.janusgraph.diskstorage.util.WriteByteBuffer;
import org.janusgraph.diskstorage.util.time.TimestampProviders;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.log.LogTxStatus;
import org.janusgraph.graphdb.database.management.GraphCacheEvictionAction;
import org.janusgraph.graphdb.database.management.MgmtLogType;
import org.janusgraph.graphdb.database.serialize.attribute.BooleanArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.BooleanSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.ByteArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.ByteSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.CharArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.CharacterSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.DateSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.DoubleArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.DoubleSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.DurationSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.EnumSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.FloatArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.FloatSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.InstantSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.IntArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.IntegerSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.JsonSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.LongArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.LongSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.ObjectSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.ParameterArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.ParameterSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.SerializableSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.ShortArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.ShortSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.StandardTransactionIdSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.StringArraySerializer;
import org.janusgraph.graphdb.database.serialize.attribute.StringSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.TypeDefinitionDescriptionSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.UUIDSerializer;
import org.janusgraph.graphdb.internal.ElementCategory;
import org.janusgraph.graphdb.internal.JanusGraphSchemaCategory;
import org.janusgraph.graphdb.internal.Order;
import org.janusgraph.graphdb.internal.RelationCategory;
import org.janusgraph.graphdb.log.StandardTransactionId;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionDescription;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardSerializer implements AttributeHandler, Serializer {

    /**
     * This offset is used by user registration to make sure they don't collide with internal class
     * registrations. It must be ensured that this number is LARGER than any internally used
     * registration number.
     * This value may NEVER EVER be changed or compatibility to older versions breaks
     */
    private static final int CLASS_REGISTRATION_OFFSET = 100;
    private static final int MAX_REGISTRATION_NO = 100000;


    private final BiMap<Integer,Class> registrations;
    private final Map<Class,AttributeSerializer> handlers;

    public StandardSerializer() {
        handlers = new HashMap<>(60);
        registrations = HashBiMap.create(60);

        //Setup
        registerClassInternal(1, Object.class, new ObjectSerializer());

        //Primitive data types
        registerClassInternal(10, Byte.class, new ByteSerializer());
        registerClassInternal(11,Short.class, new ShortSerializer());
        registerClassInternal(12,Integer.class, new IntegerSerializer());
        registerClassInternal(13,Long.class, new LongSerializer());

        registerClassInternal(14,Character.class, new CharacterSerializer());
        registerClassInternal(15,Boolean.class, new BooleanSerializer());
        registerClassInternal(16,Date.class, new DateSerializer());

        registerClassInternal(17,Geoshape.class, new GeoshapeSerializer());
        registerClassInternal(18,String.class, new StringSerializer()); //supports null serialization
        registerClassInternal(19,Float.class, new FloatSerializer());
        registerClassInternal(20,Double.class, new DoubleSerializer());
        registerClassInternal(21,UUID.class, new UUIDSerializer());

        //Arrays (support null serialization)
        registerClassInternal(22,byte[].class, new ByteArraySerializer());
        registerClassInternal(23,short[].class, new ShortArraySerializer());
        registerClassInternal(24,int[].class, new IntArraySerializer());
        registerClassInternal(25,long[].class, new LongArraySerializer());
        registerClassInternal(26,float[].class, new FloatArraySerializer());
        registerClassInternal(27,double[].class, new DoubleArraySerializer());
        registerClassInternal(28,char[].class, new CharArraySerializer());
        registerClassInternal(29,boolean[].class, new BooleanArraySerializer());
        registerClassInternal(30, String[].class, new StringArraySerializer());

        //Needed by JanusGraph
        registerClassInternal(41,TypeDefinitionCategory.class, new EnumSerializer<>(TypeDefinitionCategory.class));
        registerClassInternal(42,JanusGraphSchemaCategory.class, new EnumSerializer<>(JanusGraphSchemaCategory.class));
        registerClassInternal(43,ParameterType.class, new EnumSerializer<>(ParameterType.class));
        registerClassInternal(44,RelationCategory.class, new EnumSerializer<>(RelationCategory.class));
        registerClassInternal(45,Order.class, new EnumSerializer<>(Order.class));
        registerClassInternal(46,Multiplicity.class, new EnumSerializer<>(Multiplicity.class));
        registerClassInternal(47,Cardinality.class, new EnumSerializer<>(Cardinality.class));
        registerClassInternal(48,Direction.class, new EnumSerializer<>(Direction.class));
        registerClassInternal(49,ElementCategory.class, new EnumSerializer<>(ElementCategory.class));
        registerClassInternal(50,ConsistencyModifier.class, new EnumSerializer<>(ConsistencyModifier.class));
        registerClassInternal(51,SchemaStatus.class, new EnumSerializer<>(SchemaStatus.class));
        registerClassInternal(52,LogTxStatus.class, new EnumSerializer<>(LogTxStatus.class));
        registerClassInternal(53,MgmtLogType.class, new EnumSerializer<>(MgmtLogType.class));
        registerClassInternal(54,TimestampProviders.class, new EnumSerializer<>(TimestampProviders.class));
        registerClassInternal(55,TimeUnit.class, new EnumSerializer<>(TimeUnit.class));
        registerClassInternal(56,Mapping.class, new EnumSerializer<>(Mapping.class));
        registerClassInternal(57,ConflictAvoidanceMode.class, new EnumSerializer<>(ConflictAvoidanceMode.class));

        registerClassInternal(60,Class.class, new ClassSerializer());
        registerClassInternal(61,Parameter.class, new ParameterSerializer());
        registerClassInternal(62,Parameter[].class, new ParameterArraySerializer());
        registerClassInternal(63,TypeDefinitionDescription.class, new TypeDefinitionDescriptionSerializer());
        //Needed for configuration and transaction logging
        registerClassInternal(64,Duration.class, new DurationSerializer());
        registerClassInternal(65,Instant.class, new InstantSerializer());
        registerClassInternal(66,StandardTransactionId.class, new StandardTransactionIdSerializer());
        registerClassInternal(67,TraverserSet.class, new SerializableSerializer());
        registerClassInternal(68,HashMap.class, new SerializableSerializer());
        registerClassInternal(69,GraphCacheEvictionAction.class, new EnumSerializer<>(GraphCacheEvictionAction.class));
        registerClassInternal(70, ObjectNode.class, new JsonSerializer<>(ObjectNode.class));
        registerClassInternal(71, ArrayNode.class, new JsonSerializer<>(ArrayNode.class));
    }

    @Override
    public synchronized <V> void registerClass(int registrationNo, Class<V> datatype, AttributeSerializer<V> serializer) {
        Preconditions.checkArgument(registrationNo >= 0 && registrationNo < MAX_REGISTRATION_NO, "Registration number" +
                " out of range [0,%s]: %s", MAX_REGISTRATION_NO, registrationNo);

        if (datatype == HashMap.class) {
            final Integer hashMapRegNo = registrations.inverse().get(normalizeDataType(HashMap.class));
            if (hashMapRegNo != null) {
            	// Remove the default HashMap serializer so we can replace it with the custom one
            	registrations.remove(hashMapRegNo);
            	handlers.remove(datatype);
            }
        }

        registerClassInternal(CLASS_REGISTRATION_OFFSET + registrationNo, datatype, serializer);
    }

    public synchronized <V> void registerClassInternal(int registrationNo, Class<? extends V> datatype, AttributeSerializer<V> serializer) {
        Preconditions.checkArgument(registrationNo>0); //must be bigger than 0 since 0 is used to indicate null values
        Preconditions.checkNotNull(datatype);
        Preconditions.checkArgument(!handlers.containsKey(datatype), "DataType has already been registered: %s", datatype);
        Preconditions.checkArgument(!registrations.containsKey(registrationNo), "A datatype has already been registered for no: %s",registrationNo);
        Preconditions.checkNotNull(serializer,"Need to provide a serializer for datatype: %s",datatype);
        registrations.put(registrationNo, datatype);
        if (serializer instanceof SerializerInjected) ((SerializerInjected)serializer).setSerializer(this);
        handlers.put(datatype, serializer);
    }

    private static Class normalizeDataType(Class datatype) {
        Class superClass = datatype.getSuperclass();
        if (null != superClass && superClass.isEnum()) return superClass;
        if (Instant.class.equals(datatype)) return Instant.class;
        return datatype;
    }

    @Override
    public boolean validDataType(Class datatype) {
        return handlers.containsKey(normalizeDataType(datatype));
    }

    private<T> AttributeSerializer<T> getSerializer(Class<T> datatype) {
        AttributeSerializer<T> serializer = handlers.get(normalizeDataType(datatype));
        Preconditions.checkArgument(serializer!=null,"Datatype is not supported by database since no serializer has been registered: %s",datatype);
        return serializer;
    }

    private int getDataTypeRegistration(Class datatype) {
        Integer registrationNo = registrations.inverse().get(normalizeDataType(datatype));
        Preconditions.checkArgument(registrationNo!=null,"Datatype is not supported by database since no serializer has been registered: %s",datatype);
        assert registrationNo>0;
        return registrationNo;
    }

    private Class getDataType(int registrationNo) {
        Class clazz = registrations.get(registrationNo);
        Preconditions.checkArgument(clazz!=null,"Encountered missing datatype registration for number: %s",registrationNo);
        return clazz;
    }

    @Override
    public <V> void verifyAttribute(Class<V> datatype, Object value) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(value);
        AttributeSerializer handler = getSerializer(datatype);
        if (handler!=null) handler.verifyAttribute(value);
    }

    @Override
    public <V> V convert(Class<V> datatype, Object value) {
        Preconditions.checkNotNull(datatype);
        Preconditions.checkNotNull(value);
        AttributeSerializer handler = getSerializer(datatype);
        if (handler!=null) return (V)handler.convert(value);
        else return null;
    }

    @Override
    public boolean isOrderPreservingDatatype(Class<?> datatype) {
        return (getSerializer(datatype) instanceof OrderPreservingSerializer);
    }

    private static<V> OrderPreservingSerializer<V> ensureOrderPreserving(AttributeSerializer<V> serializer, Class<V> type) {
        Preconditions.checkArgument(serializer instanceof OrderPreservingSerializer,"Registered serializer for datatype does not support order: %s",type);
        return (OrderPreservingSerializer)serializer;
    }

    private boolean supportsNullSerialization(Class type) {
        return getSerializer(type) instanceof SupportsNullSerializer;
    }

    @Override
    public <T> T readObjectByteOrder(ScanBuffer buffer, Class<T> type) {
        return readObjectInternal(buffer,type,true);
    }

    @Override
    public <T> T readObject(ScanBuffer buffer, Class<T> type) {
        return readObjectInternal(buffer,type,false);
    }

    @Override
    public <T> T readObjectNotNull(ScanBuffer buffer, Class<T> type) {
        return readObjectNotNullInternal(buffer,type,false);
    }

    private <T> T readObjectInternal(ScanBuffer buffer, Class<T> type, boolean byteOrder) {
        if (supportsNullSerialization(type)) {
            AttributeSerializer<T> s = getSerializer(type);
            if (byteOrder) return ensureOrderPreserving(s,type).readByteOrder(buffer);
            else return s.read(buffer);
        } else {
            //Read flag for null or not
            byte flag = buffer.getByte();
            if (flag==-1) {
                return null;
            } else {
                Preconditions.checkArgument(flag==0,"Invalid flag encountered in serialization: %s. Corrupted data.",flag);
                return readObjectNotNullInternal(buffer,type,byteOrder);
            }
        }
    }

    private <T> T readObjectNotNullInternal(ScanBuffer buffer, Class<T> type, boolean byteOrder) {
        AttributeSerializer<T> s = getSerializer(type);
        if (byteOrder) {
            return ensureOrderPreserving(s,type).readByteOrder(buffer);
        } else {
            return s.read(buffer);
        }
    }

    @Override
    public Object readClassAndObject(ScanBuffer buffer) {
        long registrationNo = VariableLong.readPositive(buffer);
        if (registrationNo==0) return null;
        Class datatype = getDataType((int)registrationNo);
        return readObjectNotNullInternal(buffer, datatype, false);
    }

    @Override
    public DataOutput getDataOutput(int initialCapacity) {
        return new StandardDataOutput(initialCapacity);
    }

    @Override
    public void close() throws IOException {
        //Nothing to close
    }

    private class StandardDataOutput extends WriteByteBuffer implements DataOutput {

        private StandardDataOutput(int initialCapacity) {
            super(initialCapacity);
        }

        @Override
        public DataOutput writeObjectByteOrder(Object object, Class type) {
            Preconditions.checkArgument(StandardSerializer.this.isOrderPreservingDatatype(type),"Invalid serializer for class: %s",type);
            return writeObjectInternal(object,type,true);
        }

        @Override
        public DataOutput writeObject(Object object, Class type) {
            return writeObjectInternal(object,type,false);
        }

        @Override
        public DataOutput writeObjectNotNull(Object object) {
            return writeObjectNotNullInternal(object,false);
        }

        private DataOutput writeObjectInternal(Object object, Class type, boolean byteOrder) {
            if (supportsNullSerialization(type)) {
                AttributeSerializer s = getSerializer(type);
                if (byteOrder) ensureOrderPreserving(s,type).writeByteOrder(this,object);
                else s.write(this, object);
            } else {
                //write flag for null or not
                if (object==null) {
                    putByte((byte)-1);
                } else {
                    putByte((byte)0);
                    writeObjectNotNullInternal(object,byteOrder);
                }
            }
            return this;
        }

        private DataOutput writeObjectNotNullInternal(Object object, boolean byteOrder) {
            Preconditions.checkNotNull(object);
            Class type = object.getClass();
            AttributeSerializer s = getSerializer(type);
            if (byteOrder) {
                ensureOrderPreserving(s,type).writeByteOrder(this,object);
            } else {
                s.write(this, object);
            }
            return this;
        }

        @Override
        public DataOutput writeClassAndObject(Object object) {
            if (object==null) VariableLong.writePositive(this,0);
            else {
                Class type = object.getClass();
                VariableLong.writePositive(this,getDataTypeRegistration(type));
                writeObjectNotNullInternal(object,false);
            }
            return this;
        }

        @Override
        public DataOutput putLong(long val) {
            super.putLong(val);
            return this;
        }

        @Override
        public DataOutput putInt(int val) {
            super.putInt(val);
            return this;
        }

        @Override
        public DataOutput putShort(short val) {
            super.putShort(val);
            return this;
        }

        @Override
        public WriteBuffer putBoolean(boolean val) {
            super.putBoolean(val);
            return this;
        }

        @Override
        public DataOutput putByte(byte val) {
            super.putByte(val);
            return this;
        }

        @Override
        public DataOutput putBytes(byte[] val) {
            super.putBytes(val);
            return this;
        }

        @Override
        public DataOutput putBytes(final StaticBuffer val) {
            super.putBytes(val);
            return this;
        }

        @Override
        public DataOutput putChar(char val) {
            super.putChar(val);
            return this;
        }

        @Override
        public DataOutput putFloat(float val) {
            super.putFloat(val);
            return this;
        }

        @Override
        public DataOutput putDouble(double val) {
            super.putDouble(val);
            return this;
        }

    }

    private class ClassSerializer implements OrderPreservingSerializer<Class>, SupportsNullSerializer {

        private final IntegerSerializer ints = new IntegerSerializer();

        @Override
        public Class readByteOrder(ScanBuffer buffer) {
            return getClass(ints.readByteOrder(buffer));
        }

        @Override
        public void writeByteOrder(WriteBuffer buffer, Class attribute) {
            ints.writeByteOrder(buffer, attribute == null ? 0 : getDataTypeRegistration(attribute));
        }

        @Override
        public Class read(ScanBuffer buffer) {
            return getClass(VariableLong.readPositive(buffer));
        }

        private Class getClass(long registrationNo) {
            assert registrationNo<Integer.MAX_VALUE && registrationNo>=0;
            if (registrationNo==0) return null;
            return getDataType((int) registrationNo);
        }

        @Override
        public void write(WriteBuffer buffer, Class attribute) {
            VariableLong.writePositive(buffer,attribute==null?0:getDataTypeRegistration(attribute));
        }

        @Override
        public void verifyAttribute(Class value) {
            //Accept all values
        }

        @Override
        public Class convert(Object value) {
            if (value instanceof Class) return (Class)value;
            else return null;
        }
    }

}

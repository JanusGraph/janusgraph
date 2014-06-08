package com.thinkaurelius.titan.graphdb.database.serialize;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.attribute.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ReadBuffer;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.diskstorage.util.WriteByteBuffer;
import com.thinkaurelius.titan.graphdb.database.serialize.kryo.KryoSerializer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardSerializer extends StandardAttributeHandling implements Serializer {

    private final KryoSerializer backupSerializer;

    public StandardSerializer(boolean allowCustomSerialization) {
        if (allowCustomSerialization) backupSerializer = new KryoSerializer(DEFAULT_REGISTRATIONS);
        else backupSerializer = null;
    }

    public StandardSerializer() {
        this(true);
    }

    private KryoSerializer getBackupSerializer() {
        Preconditions.checkState(backupSerializer!=null,"Serializer is not configured for custom object serialization");
        return backupSerializer;
    }

    private boolean supportsNullSerialization(Class type) {
        return getSerializer(type) instanceof SupportsNullSerializer;
    }


    @Override
    public <T> T readObjectByteOrder(ReadBuffer buffer, Class<T> type) {
        return readObjectInternal(buffer,type,true);
    }

    @Override
    public <T> T readObject(ReadBuffer buffer, Class<T> type) {
        return readObjectInternal(buffer,type,false);
    }

    @Override
    public <T> T readObjectNotNull(ReadBuffer buffer, Class<T> type) {
        return readObjectNotNullInternal(buffer,type,false);
    }

    private <T> T readObjectInternal(ReadBuffer buffer, Class<T> type, boolean byteOrder) {
        if (supportsNullSerialization(type)) {
            AttributeSerializer<T> s = getSerializer(type);
            if (byteOrder) return ((OrderPreservingSerializer<T>)s).readByteOrder(buffer);
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

    private <T> T readObjectNotNullInternal(ReadBuffer buffer, Class<T> type, boolean byteOrder) {
        AttributeSerializer<T> s = getSerializer(type);
        if (byteOrder) {
            Preconditions.checkArgument(s!=null && s instanceof OrderPreservingSerializer,"Invalid serializer for class: %s",type);
            return ((OrderPreservingSerializer<T>)s).readByteOrder(buffer);
        } else {
            if (s!=null) return s.read(buffer);
            else return getBackupSerializer().readObjectNotNull(buffer,type);
        }
    }

    @Override
    public Object readClassAndObject(ReadBuffer buffer) {
        return getBackupSerializer().readClassAndObject(buffer);
    }

    @Override
    public DataOutput getDataOutput(int initialCapacity) {
        return new StandardDataOutput(initialCapacity);
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
                if (byteOrder) ((OrderPreservingSerializer)s).writeByteOrder(this,object);
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
            AttributeSerializer s = getSerializer(object.getClass());
            if (byteOrder) {
                Preconditions.checkArgument(s!=null && s instanceof OrderPreservingSerializer,"Invalid serializer for class: %s",object.getClass());
                ((OrderPreservingSerializer)s).writeByteOrder(this,object);
            } else {
                if (s!=null) s.write(this, object);
                else getBackupSerializer().writeObjectNotNull(this,object);
            }
            return this;
        }

        @Override
        public DataOutput writeClassAndObject(Object object) {
            getBackupSerializer().writeClassAndObject(this,object);
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

}

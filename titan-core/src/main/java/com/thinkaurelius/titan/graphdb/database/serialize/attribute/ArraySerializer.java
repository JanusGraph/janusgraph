package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.SupportsNullSerializer;

import java.lang.reflect.Array;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class ArraySerializer implements SupportsNullSerializer {

    protected abstract Object getArray(int length);

    protected abstract void setArray(Object array, int pos, Object value);

    <T> T convertInternal(Object value, Class primitiveClass, Class boxedClass) {
        int size;
        if (value==null) {
            return null;
        } if (primitiveClass!=null && (value.getClass().isArray()) && (value.getClass().getComponentType().equals(primitiveClass))) {
            //primitive array of the right type
            return (T)value;
        } else if ((size=isIterableOf(value, boxedClass))>=0) {
            //Iterable of the right (boxed) type and no null values
            Object array = getArray(size);
            int pos=0;
            for (Object element : (Iterable)value)
                setArray(array,pos++,element);
            return (T)array;
        } else if ((size=isArrayOf(value, boxedClass))>=0) {
            //array of the right (boxed) type and no null values
            Object array = getArray(size);
            for (int i = 0; i < size; i++) {
                setArray(array,i,Array.get(value, i));
            }
            return (T)array;
        }
        return null;
    }

    protected int isIterableOf(Object value, Class boxedClass) {
        if (!(value instanceof Iterable)) return -1;
        Iterable c = (Iterable)value;
        int size = 0;
        for (Object element : c) {
            if (element==null || !element.getClass().equals(boxedClass)) return -1;
            size++;
        }
        return size;
    }

    protected int isArrayOf(Object value, Class boxedClass) {
        if (!value.getClass().isArray() ||
                !value.getClass().getComponentType().equals(boxedClass)) return -1;
        for (int i = 0; i < Array.getLength(value); i++) {
            if (Array.get(value,i)==null) return -1;
            assert Array.get(value,i).getClass().equals(boxedClass);
        }
        return Array.getLength(value);
    }

    //############### Serialization ###################

    protected int getLength(ScanBuffer buffer) {
        long length = VariableLong.readPositive(buffer)-1;
        Preconditions.checkArgument(length >= -1 && length <= Integer.MAX_VALUE);
        return (int)length;
    }

    protected void writeLength(WriteBuffer buffer, Object array) {
        if (array==null) VariableLong.writePositive(buffer,0);
        else {
            long length = ((long)Array.getLength(array))+1;
            assert length>0;
            VariableLong.writePositive(buffer,length);
        }
    }

}

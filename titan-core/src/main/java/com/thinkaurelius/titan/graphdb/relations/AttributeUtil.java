package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.attribute.FullDouble;
import com.thinkaurelius.titan.core.attribute.FullFloat;
import com.thinkaurelius.titan.core.attribute.Geoshape;
import com.thinkaurelius.titan.core.attribute.Interval;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.DoubleSerializer;
import com.thinkaurelius.titan.graphdb.database.serialize.attribute.FloatSerializer;

import java.lang.reflect.Array;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class AttributeUtil {

    public static final Object verifyAttributeQuery(TitanKey key, Object attribute) {
        if (attribute==null) return attribute;
        else if (attribute instanceof Interval) {
            Comparable start = (Comparable) verifyAttributeQuery(key,((Interval)attribute).getStart());
            Comparable end = (Comparable) verifyAttributeQuery(key,((Interval)attribute).getEnd());
            return new Interval(start,end);
        } else return verifyAttribute(key,attribute);
    }


    public static final Object verifyAttribute(TitanKey key, Object attribute) {
        attribute = prepareAttribute(attribute, key.getDataType());
        checkAttributeType(key, attribute);
        return attribute;
    }

    private static final Object prepareAttribute(Object attribute, Class<?> datatype) {
        Preconditions.checkNotNull(attribute, "Attribute cannot be null");
        if (!datatype.equals(Object.class)) {
            if ((attribute instanceof Short || attribute instanceof Byte) && datatype.equals(Integer.class)) {
                attribute = ((Number)attribute).intValue();
            } else if ((attribute instanceof Integer || attribute instanceof Short || attribute instanceof Byte) && datatype.equals(Long.class)) {
                attribute = ((Number)attribute).longValue();
            } else if (attribute instanceof Number && datatype.equals(Double.class)) {
                if (!(attribute instanceof Double)) attribute = ((Number)attribute).doubleValue();
            } else if (attribute instanceof Number && datatype.equals(Float.class)) {
                if (!(attribute instanceof Float)) attribute = ((Number)attribute).floatValue();
            } else if (attribute instanceof Number && datatype.equals(FullDouble.class)) {
                if (!(attribute instanceof FullDouble)) attribute = new FullDouble(((Number)attribute).doubleValue());
            } else if (attribute instanceof Number && datatype.equals(FullFloat.class)) {
                if (!(attribute instanceof FullFloat)) attribute = new FullFloat(((Number)attribute).floatValue());
            } else if (datatype.equals(Geoshape.class) && !(attribute instanceof Geoshape)) {
                if (attribute.getClass().isArray() && (attribute.getClass().getComponentType().isPrimitive() ||
                        Number.class.isAssignableFrom(attribute.getClass().getComponentType())) ) {
                    int len= Array.getLength(attribute);
                    double[] arr = new double[len];
                    for (int i=0;i<len;i++) arr[i]=((Number)Array.get(attribute,i)).doubleValue();
                    if (arr.length==2) attribute=Geoshape.point(arr[0],arr[1]);
                    else if (arr.length==3) attribute=Geoshape.circle(arr[0],arr[1],arr[2]);
                    else if (arr.length==4) attribute=Geoshape.box(arr[0],arr[1],arr[2],arr[3]);
                }
            }
        }
        return attribute;
    }



    private static final void checkAttributeType(TitanKey key, Object attribute) {
        Class<?> datatype = key.getDataType();
        if (!datatype.equals(Object.class)) {
            Preconditions.checkArgument(datatype.equals(attribute.getClass()),
                    "Value [%s] is not an instance of the expected data type for property key [%s]. Expected: %s, found: %s", attribute,
                    key.getName(), datatype, attribute.getClass());
            if (datatype.equals(Double.class)) {
                Preconditions.checkArgument(DoubleSerializer.withinRange((Double)attribute),
                        "Double value is not within value range [%s,%s]: %s",DoubleSerializer.MIN_VALUE,DoubleSerializer.MAX_VALUE,attribute);
            } else if (datatype.equals(Float.class)) {
                Preconditions.checkArgument(FloatSerializer.withinRange((Float) attribute),
                        "Float value is not within value range [%s,%s]: %s",FloatSerializer.MIN_VALUE,FloatSerializer.MAX_VALUE,attribute);
            }
        }
    }

}

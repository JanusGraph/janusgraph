package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.attribute.Geoshape;

import java.lang.reflect.Array;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GeoshapeHandler implements AttributeHandler<Geoshape> {

    @Override
    public void verifyAttribute(Geoshape value) {
        //All values of Geoshape are valid
    }

    @Override
    public Geoshape convert(Object value) {
        if (value.getClass().isArray() && (value.getClass().getComponentType().isPrimitive() ||
                Number.class.isAssignableFrom(value.getClass().getComponentType())) ) {
            Geoshape shape = null;
            int len= Array.getLength(value);
            double[] arr = new double[len];
            for (int i=0;i<len;i++) arr[i]=((Number)Array.get(value,i)).doubleValue();
            if (len==2) shape=Geoshape.point(arr[0],arr[1]);
            else if (len==3) shape=Geoshape.circle(arr[0],arr[1],arr[2]);
            else if (len==4) shape=Geoshape.box(arr[0],arr[1],arr[2],arr[3]);
            else throw new IllegalArgumentException("Expected 2-4 coordinates to create Geoshape, but given: " + value);
            return shape;
        } else if (value instanceof String) {
            String[] components=null;
            for (String delimiter : new String[]{",",";"}) {
                components = ((String)value).split(delimiter);
                if (components.length>=2 && components.length<=4) break;
                else components=null;
            }
            Preconditions.checkArgument(components!=null,"Could not parse coordinates from string: %s",value);
            double[] coords = new double[components.length];
            try {
                for (int i=0;i<components.length;i++) {
                    coords[i]=Double.parseDouble(components[i]);
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse coordinates from string: " + value, e);
            }
            return convert(coords);
        } else return null;
    }
}

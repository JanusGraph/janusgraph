package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.attribute.FullFloat;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FullFloatHandler implements AttributeHandler<FullFloat> {

    @Override
    public void verifyAttribute(FullFloat value) {
        //All values are valid
    }

    //Copied from FloatSerializer
    @Override
    public FullFloat convert(Object value) {
        if (value instanceof Number) {
            double d = ((Number)value).doubleValue();
            if (d<Float.MIN_VALUE || d>Float.MAX_VALUE) throw new IllegalArgumentException("Value too large for float: " + value);
            return new FullFloat((float)d);
        } else if (value instanceof String) {
            return new FullFloat(Float.parseFloat((String) value));
        } else return null;
    }
}

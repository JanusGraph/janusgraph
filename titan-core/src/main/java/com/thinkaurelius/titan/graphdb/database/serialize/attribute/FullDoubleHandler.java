package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.thinkaurelius.titan.core.AttributeHandler;
import com.thinkaurelius.titan.core.attribute.FullDouble;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class FullDoubleHandler implements AttributeHandler<FullDouble> {

    @Override
    public void verifyAttribute(FullDouble value) {
        //All values are valid
    }

    //Copied from DoubleSerializer
    @Override
    public FullDouble convert(Object value) {
        if (value instanceof Number) {
            return new FullDouble(((Number)value).doubleValue());
        } else if (value instanceof String) {
            return new FullDouble(Double.parseDouble((String)value));
        } else return null;
    }
}

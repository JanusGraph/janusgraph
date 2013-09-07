package com.thinkaurelius.titan.graphdb.database.serialize.attribute;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.AttributeSerializer;
import com.thinkaurelius.titan.diskstorage.ScanBuffer;
import com.thinkaurelius.titan.diskstorage.WriteBuffer;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StringSerializer implements AttributeSerializer<String> {

    @Override
    public String read(ScanBuffer buffer) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void writeObjectData(WriteBuffer buffer, String attribute) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void verifyAttribute(String value) {
        //Check that it contains no null-characters

    }

    @Override
    public String convert(Object value) {
        Preconditions.checkNotNull(value);
        return value.toString();
    }

}

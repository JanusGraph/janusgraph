package com.thinkaurelius.titan.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.util.encoding.StringEncoding;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum EntryMetaData {

    TTL, VISIBILITY, TIMESTAMP;


    public static final java.util.Map<EntryMetaData,Object> EMPTY_METADATA = ImmutableMap.of();

    public Class getDataType() {
        switch(this) {
            case VISIBILITY: return String.class;
            case TTL:
                return Integer.class;
            case TIMESTAMP: return Long.class;
            default: throw new AssertionError("Unexpected meta data: " + this);
        }
    }

    public boolean isValidData(Object data) {
        Preconditions.checkNotNull(data);
        switch(this) {
            case VISIBILITY:
                if (!(data instanceof String)) return false;
                return StringEncoding.isAsciiString((String)data);
            case TTL:
                return data instanceof Integer && ((Integer) data) >= 0L;
            case TIMESTAMP:
                return data instanceof Long;
            default: throw new AssertionError("Unexpected meta data: " + this);
        }
    }

    public boolean isIdentifying() {
        switch(this) {
            case VISIBILITY:
                return true;
            case TTL:
            case TIMESTAMP:
                return false;
            default: throw new AssertionError("Unexpected meta data: " + this);
        }
    }

    public static final List<EntryMetaData> IDENTIFYING_METADATA = new ArrayList<EntryMetaData>(3) {{
        for (EntryMetaData meta : values()) if (meta.isIdentifying()) add(meta);
    }};

    public static class Map extends EnumMap<EntryMetaData,Object> {

        public Map() {
            super(EntryMetaData.class);
        }

        @Override
        public Object put(EntryMetaData key, Object value) {
            Preconditions.checkArgument(key.isValidData(value),"Invalid meta data [%s] for [%s]",value,key);
            return super.put(key,value);
        }

    }

}

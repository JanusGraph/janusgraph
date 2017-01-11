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

package org.janusgraph.diskstorage;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.janusgraph.util.encoding.StringEncoding;

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

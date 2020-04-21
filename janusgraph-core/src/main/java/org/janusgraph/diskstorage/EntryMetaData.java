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
import org.janusgraph.util.encoding.StringEncoding;

import java.util.Collections;
import java.util.EnumMap;
import java.util.function.Function;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Alexander Patrikalakis (amcp@mit.edu)
 */
public enum EntryMetaData {

    TTL(Integer.class, false, data -> data instanceof Integer && ((Integer) data) >= 0L),
    VISIBILITY(String.class, true, data -> data instanceof String && StringEncoding.isAsciiString((String) data)),
    TIMESTAMP(Long.class, false, data -> data instanceof Long);

    public static final java.util.Map<EntryMetaData,Object> EMPTY_METADATA = Collections.emptyMap();

    private final Class<?> dataType;
    private final boolean identifying;
    private final Function<Object, Boolean> validator;

    EntryMetaData(final Class<?> dataType, final boolean identifying, final Function<Object, Boolean> validator) {
        this.dataType = dataType;
        this.identifying = identifying;
        this.validator = validator;
    }

    public Class<?> getDataType() {
        return dataType;
    }

    public boolean isIdentifying() {
        return identifying;
    }

    /**
     * Validates a datum according to the metadata type.
     * @param datum object to validate
     * @return true if datum is a valid instance of this type and false otherwise.
     */
    public boolean isValidData(Object datum) {
        Preconditions.checkNotNull(datum);
        return validator.apply(datum);
    }

    /**
     * EntryMetaData.Map extends EnumMap to add validation prior to invoking the superclass EnumMap::put(k,v) method.
     */
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

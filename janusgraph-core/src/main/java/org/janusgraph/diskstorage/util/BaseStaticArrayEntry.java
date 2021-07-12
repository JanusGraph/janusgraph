// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.diskstorage.util;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryMetaData;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.graphdb.relations.RelationCache;

import java.util.Map;

class BaseStaticArrayEntry extends StaticArrayBuffer implements Entry {

    private final int valuePosition;

    public BaseStaticArrayEntry(byte[] array, int offset, int limit, int valuePosition) {
        super(array, offset, limit);
        Preconditions.checkArgument(valuePosition > 0);
        Preconditions.checkArgument(valuePosition <= length());
        this.valuePosition = valuePosition;
    }

    public BaseStaticArrayEntry(byte[] array, int limit, int valuePosition) {
        this(array, 0, limit, valuePosition);
    }

    public BaseStaticArrayEntry(byte[] array, int valuePosition) {
        this(array, 0, array.length, valuePosition);
    }

    public BaseStaticArrayEntry(StaticBuffer buffer, int valuePosition) {
        super(buffer);
        Preconditions.checkArgument(valuePosition > 0);
        Preconditions.checkArgument(valuePosition <= length());
        this.valuePosition = valuePosition;
    }

    @Override
    public int getValuePosition() {
        return valuePosition;
    }

    @Override
    public boolean hasValue() {
        return valuePosition < length();
    }

    @Override
    public StaticBuffer getColumn() {
        return getColumnAs(StaticBuffer.STATIC_FACTORY);
    }

    @Override
    public <T> T getColumnAs(Factory<T> factory) {
        return super.as(factory, 0, valuePosition);
    }

    @Override
    public StaticBuffer getValue() {
        return getValueAs(StaticBuffer.STATIC_FACTORY);
    }

    @Override
    public <T> T getValueAs(Factory<T> factory) {
        return super.as(factory, valuePosition, super.length() - valuePosition);
    }

    //Override from StaticArrayBuffer to restrict to column

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (!(o instanceof StaticBuffer)) return false;
        final Entry b = (Entry) o;
        return getValuePosition() == b.getValuePosition() && compareTo(getValuePosition(), b, getValuePosition()) == 0;
    }

    @Override
    public int hashCode() {
        return hashCode(getValuePosition());
    }

    @Override
    public int compareTo(StaticBuffer other) {
        int otherLen = (other instanceof Entry) ? ((Entry) other).getValuePosition() : other.length();
        return compareTo(getValuePosition(), other, otherLen);
    }

    @Override
    public String toString() {
        String s = super.toString();
        int pos = getValuePosition() * 2 + 2;
        StringBuilder sb = new StringBuilder(s.substring(0, pos));
        if (hasValue()) {
            return sb.append("->0x").append(s.substring(pos)).toString();
        } else {
            return sb.append("->[no value]").toString();
        }
    }

    //########## CACHE ############

    @Override
    public RelationCache getCache() {
        return null;
    }

    @Override
    public void setCache(RelationCache cache) {
        throw new UnsupportedOperationException();
    }

    //########## META DATA ############

    @Override
    public boolean hasMetaData() {
        return false;
    }

    @Override
    public Map<EntryMetaData, Object> getMetaData() {
        return EntryMetaData.EMPTY_METADATA;
    }

}

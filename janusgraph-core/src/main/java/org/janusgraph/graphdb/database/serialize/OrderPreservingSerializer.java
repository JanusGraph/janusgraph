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

package org.janusgraph.graphdb.database.serialize;

import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;

/**
 * Interface that extends {@link AttributeSerializer} to provide a serialization that is byte order preserving, i.e. the
 * order of the elements (as given by its {@link Comparable} implementation) corresponds to the natural order of the
 * serialized byte representation representation.
 *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface OrderPreservingSerializer<V> extends AttributeSerializer<V> {


    /**
     * Reads an attribute from the given ReadBuffer assuming it was written in byte order.
     * <p>
     * It is expected that this read operation adjusts the position in the ReadBuffer to after the attribute value.
     *
     * @param buffer ReadBuffer to read attribute from
     * @return Read attribute
     */
    V readByteOrder(ScanBuffer buffer);

    /**
     * Writes the attribute value to the given WriteBuffer such that the byte order of the result is equal to the
     * natural order of the attribute.
     * <p>
     * It is expected that this write operation adjusts the position in the WriteBuffer to after the attribute value.
     *
     * @param buffer    WriteBuffer to write attribute to
     * @param attribute Attribute to write to WriteBuffer
     */
    void writeByteOrder(WriteBuffer buffer, V attribute);

}

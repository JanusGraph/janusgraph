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

package org.janusgraph.core.attribute;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.ScanBuffer;
import org.janusgraph.diskstorage.WriteBuffer;


/**
 * Allows custom serializer definitions for attribute values.
 * <p>
 * For most data types (i.e. classes) used with properties, using the default serializer when registering the type with the
 * JanusGraph will be sufficient and efficient in practice. However, for certain data types, it can be more
 * efficient to provide custom serializers implementing this interface.
 * Such custom serializers are registered in the configuration file by specifying their path and loaded when
 * the database is initialized. Hence, the serializer must be on the classpath.
 * <br>
 * <p>
 * When a {@link org.janusgraph.core.PropertyKey} is defined using a data type specified via {@link org.janusgraph.core.schema.PropertyKeyMaker} for which a custom serializer
 * is configured, then it will use this custom serializer for persistence operations.
 *
 * @param <V> Type of the attribute associated with the AttributeSerializer
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see org.janusgraph.core.schema.RelationTypeMaker
 * @see <a href="https://docs.janusgraph.org/advanced-topics/serializer/">
 *      "Datatype and Attribute Serializer Configuration" manual chapter</a>
 */
public interface AttributeSerializer<V> {

    /**
     * Reads an attribute from the given ReadBuffer.
     * <p>
     * It is expected that this read operation adjusts the position in the ReadBuffer to after the attribute value.
     *
     * @param buffer ReadBuffer to read attribute from
     * @return Read attribute
     */
    V read(ScanBuffer buffer);

    /**
     * Writes the attribute value to the given WriteBuffer.
     * <p>
     * It is expected that this write operation adjusts the position in the WriteBuffer to after the attribute value.
     *
     * @param buffer    WriteBuffer to write attribute to
     * @param attribute Attribute to write to WriteBuffer
     */
    void write(WriteBuffer buffer, V attribute);


    /**
     * Verifies the given (not-null) attribute value is valid.
     * Throws an {@link IllegalArgumentException} if the value is invalid,
     * otherwise simply returns.
     *
     * @param value to verify
     */
    default void verifyAttribute(V value) {
        Preconditions.checkNotNull(value,"Provided value cannot be null");
    }

    /**
     * Converts the given (not-null) value to the expected data type V.
     * The given object will NOT be of type V.
     * Throws an {@link IllegalArgumentException} if it cannot be converted.
     *
     * @param value to convert
     * @return converted to expected data type
     */
    default V convert(Object value) {
        try {
            return (V) value;
        } catch (ClassCastException e) {
            return null;
        }
    }

}

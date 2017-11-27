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

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface AttributeHandler {

    <T> void registerClass(int registrationNo, Class<T> type, AttributeSerializer<T> attributeHandler);

    boolean validDataType(Class datatype);

    <V> void verifyAttribute(Class<V> datatype, Object value);

    /**
     * Converts the given (not-null) value to the this datatype V.
     * The given object will NOT be of type V.
     * Throws an {@link IllegalArgumentException} if it cannot be converted.
     *
     * @param value to convert
     * @return converted to expected datatype
     */
    <V> V convert(Class<V> datatype, Object value);

    boolean isOrderPreservingDatatype(Class<?> datatype);

}

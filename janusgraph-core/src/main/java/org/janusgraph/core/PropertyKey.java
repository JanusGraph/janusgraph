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


package org.janusgraph.core;


/**
 * PropertyKey is an extension of {@link RelationType} for properties. Each property in JanusGraph has a key.
 * <p>
 * A property key defines the following characteristics of a property:
 * <ul>
 * <li><strong>Data Type:</strong> The data type of the value for a given property of this key</li>
 * <li><strong>Cardinality:</strong> The cardinality of the set of properties that may be associated with a single
 * vertex through a particular key.
 * </li>
 * </ul>
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com)
 * @see RelationType
 */
public interface PropertyKey extends RelationType {

    /**
     * Returns the data type for this property key.
     * The values of all properties of this type must be an instance of this data type.
     *
     * @return Data type for this property key.
     */
    Class<?> dataType();

    /**
     * The {@link Cardinality} of this property key.
     * @return
     */
    Cardinality cardinality();

}

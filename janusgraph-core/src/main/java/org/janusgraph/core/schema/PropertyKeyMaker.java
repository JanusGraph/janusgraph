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

package org.janusgraph.core.schema;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;

/**
 * Used to define new {@link org.janusgraph.core.PropertyKey}s.
 * An property key is defined by its name, {@link Cardinality}, its data type, and its signature - all of which
 * can be specified in this builder.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface PropertyKeyMaker extends RelationTypeMaker {

    /**
     * Configures the {@link org.janusgraph.core.Cardinality} of this property key.
     *
     * @param cardinality
     * @return this PropertyKeyMaker
     */
    public PropertyKeyMaker cardinality(Cardinality cardinality);

    /**
     * Configures the data type for this property key.
     * <p>
     * Property instances for this key will only accept values that are instances of this class.
     * Every property key must have its data type configured. Setting the data type to Object.class allows
     * any type of value but comes at the expense of longer serialization because class information
     * is stored with the value.
     * <p>
     * It is strongly advised to pick an appropriate data type class so JanusGraph can enforce it throughout the database.
     *
     * @param clazz Data type to be configured.
     * @return this PropertyKeyMaker
     * @see org.janusgraph.core.PropertyKey#dataType()
     */
    public PropertyKeyMaker dataType(Class<?> clazz);

    @Override
    public PropertyKeyMaker signature(PropertyKey... types);


    /**
     * Defines the {@link org.janusgraph.core.PropertyKey} specified by this PropertyKeyMaker and returns the resulting key.
     *
     * @return the created {@link PropertyKey}
     */
    @Override
    public PropertyKey make();
}

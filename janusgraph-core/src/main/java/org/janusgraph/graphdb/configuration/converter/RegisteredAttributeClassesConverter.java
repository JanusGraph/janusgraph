// Copyright 2018 JanusGraph Authors
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

package org.janusgraph.graphdb.configuration.converter;

import com.google.common.base.Preconditions;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.RegisteredAttributeClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Converter which converts {@link Configuration} into a List of {@link RegisteredAttributeClass}
 */
public class RegisteredAttributeClassesConverter {

    private static RegisteredAttributeClassesConverter registeredAttributeClassesConverter;

    private RegisteredAttributeClassesConverter(){}

    public static RegisteredAttributeClassesConverter getInstance(){
        if(registeredAttributeClassesConverter == null){
            registeredAttributeClassesConverter = new RegisteredAttributeClassesConverter();
        }
        return registeredAttributeClassesConverter;
    }

    public List<RegisteredAttributeClass<?>> convert(Configuration configuration) {
        List<RegisteredAttributeClass<?>> all = new ArrayList<>();
        for (String attributeId : configuration.getContainedNamespaces(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_NS)) {
            Preconditions.checkArgument(attributeId.startsWith(GraphDatabaseConfiguration.ATTRIBUTE_PREFIX),
                "Invalid attribute definition: %s",attributeId);
            int position;
            try {
                position = Integer.parseInt(attributeId.substring(GraphDatabaseConfiguration.ATTRIBUTE_PREFIX.length()));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Expected entry of the form ["+
                    GraphDatabaseConfiguration.ATTRIBUTE_PREFIX +"X] where X is a number but given" + attributeId);
            }
            final Class<?> clazz;
            final AttributeSerializer<?> serializer;
            String classname = configuration.get(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS,attributeId);
            try {
                clazz = Class.forName(classname);
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not find attribute class" + classname, e);
            }
            Preconditions.checkNotNull(clazz);

            Preconditions.checkArgument(configuration.has(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, attributeId));
            String serializerName = configuration.get(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS, attributeId);
            try {
                Class<?> serializerClass = Class.forName(serializerName);
                serializer = (AttributeSerializer) serializerClass.newInstance();
            } catch (ClassNotFoundException e) {
                throw new IllegalArgumentException("Could not find serializer class" + serializerName);
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalArgumentException("Could not instantiate serializer class" + serializerName, e);
            }
            Preconditions.checkNotNull(serializer);
            RegisteredAttributeClass reg = new RegisteredAttributeClass(position, clazz, serializer);
            for (RegisteredAttributeClass<?> registeredAttributeClass : all) {
                if (registeredAttributeClass.equals(reg)) {
                    throw new IllegalArgumentException("Duplicate attribute registration: " +
                        registeredAttributeClass + " and " + reg);
                }
            }
            all.add(reg);

        }
        return all;
    }

}

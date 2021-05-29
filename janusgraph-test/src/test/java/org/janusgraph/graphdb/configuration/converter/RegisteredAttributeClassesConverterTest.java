// Copyright 2019 JanusGraph Authors
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

import com.google.common.collect.ImmutableList;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;
import org.janusgraph.graphdb.configuration.RegisteredAttributeClass;
import org.janusgraph.graphdb.database.serialize.attribute.BooleanSerializer;
import org.janusgraph.graphdb.database.serialize.attribute.IntegerSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class RegisteredAttributeClassesConverterTest {

    private final RegisteredAttributeClassesConverter registeredAttributeClassesConverter =
        RegisteredAttributeClassesConverter.getInstance();

    @Mock
    private Configuration configuration;

    @Test
    public void shouldThrowExceptionOnNonAttributeId(){
        String attributeId = "not_attribute_id";

        when(configuration.getContainedNamespaces(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_NS))
            .thenReturn(Collections.singleton(attributeId));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> registeredAttributeClassesConverter.convert(configuration));

        assertEquals("Invalid attribute definition: "+attributeId, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionOnNonNumberAttributeId(){
        String attributeId = GraphDatabaseConfiguration.ATTRIBUTE_PREFIX+"not_number";

        when(configuration.getContainedNamespaces(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_NS))
            .thenReturn(Collections.singleton(attributeId));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> registeredAttributeClassesConverter.convert(configuration));

        assertEquals("Expected entry of the form ["+GraphDatabaseConfiguration.ATTRIBUTE_PREFIX
                +"X] where X is a number but given "+attributeId, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionOnNonFoundAttributeClass(){
        String attributeId = GraphDatabaseConfiguration.ATTRIBUTE_PREFIX+"1";
        String classname = "not_class_name";

        when(configuration.getContainedNamespaces(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_NS))
            .thenReturn(Collections.singleton(attributeId));
        when(configuration.get(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS,attributeId))
            .thenReturn(classname);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> registeredAttributeClassesConverter.convert(configuration));

        assertEquals("Could not find attribute class " + classname, exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenCustomSerializerIsNotSet(){
        String attributeId = GraphDatabaseConfiguration.ATTRIBUTE_PREFIX+"1";
        String classname = Integer.class.getName();

        when(configuration.getContainedNamespaces(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_NS))
            .thenReturn(Collections.singleton(attributeId));

        when(configuration.get(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS, attributeId)).thenReturn(classname);

        assertThrows(IllegalArgumentException.class, () -> registeredAttributeClassesConverter.convert(configuration));
    }

    @Test
    public void shouldThrowExceptionWhenSerializerClassNotFound(){
        AttributeComponents attributeComponents1 =
            makeAttributeComponents("1", Integer.class.getName(), "NoSerializerClass");

        mockAttributeComponents(ImmutableList.of(attributeComponents1));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> registeredAttributeClassesConverter.convert(configuration));

        assertEquals("Could not find serializer class " + attributeComponents1.serializerClassName,
            exception.getMessage());
    }

    @Test
    public void shouldThrowExceptionWhenSerializerClassNotSerializer(){
        AttributeComponents attributeComponents1 =
            makeAttributeComponents("1", Integer.class.getName(), Integer.class.getName());

        mockAttributeComponents(ImmutableList.of(attributeComponents1));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> registeredAttributeClassesConverter.convert(configuration));

        assertEquals("Could not instantiate serializer class " + attributeComponents1.serializerClassName,
            exception.getMessage());
    }

    @Test
    public void shouldReturnSingleRegisteredAttribute(){
        AttributeComponents attributeComponents1 =
            makeAttributeComponents("1", Integer.class.getName(), IntegerSerializer.class.getName());

        mockAttributeComponents(ImmutableList.of(attributeComponents1));

        List<RegisteredAttributeClass<?>> result = registeredAttributeClassesConverter.convert(configuration);

        assertEquals(1, result.size());
    }

    @Test
    public void shouldReturnMultipleRegisteredAttribute(){
        AttributeComponents attributeComponents1 =
            makeAttributeComponents("1", Integer.class.getName(), IntegerSerializer.class.getName());

        AttributeComponents attributeComponents2 =
            makeAttributeComponents("2", Boolean.class.getName(), BooleanSerializer.class.getName());

        mockAttributeComponents(ImmutableList.of(attributeComponents1, attributeComponents2));

        List<RegisteredAttributeClass<?>> result = registeredAttributeClassesConverter.convert(configuration);

        assertEquals(2, result.size());
    }

    @Test
    public void shouldThrowExceptionOnDuplicateAttributes(){
        AttributeComponents attributeComponents1 =
            makeAttributeComponents("1", Integer.class.getName(), IntegerSerializer.class.getName());

        AttributeComponents attributeComponents2 =
            makeAttributeComponents("2", Integer.class.getName(), IntegerSerializer.class.getName());

        mockAttributeComponents(ImmutableList.of(attributeComponents1, attributeComponents2));

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
            () -> registeredAttributeClassesConverter.convert(configuration));

        assertEquals("Duplicate attribute registration: "+Integer.class, exception.getMessage());
    }

    private void mockAttributeComponents(Collection<AttributeComponents> attributeComponents){

        Set<String> attributeIds = attributeComponents.stream().map(o -> o.attributeId).collect(Collectors.toSet());

        Map<String, String> classNames = attributeComponents.stream()
            .collect(Collectors.toMap(o -> o.attributeId, o -> o.classname));

        Map<String, String> serializerClassNames = attributeComponents.stream()
            .collect(Collectors.toMap(o -> o.attributeId, o -> o.serializerClassName));

        when(configuration.getContainedNamespaces(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_NS))
            .thenReturn(attributeIds);

        when(configuration.get(any(), any())).thenAnswer(invocation -> {
            Object argument1 = invocation.getArguments()[0];
            Object argument2 = invocation.getArguments()[1];
            if(GraphDatabaseConfiguration.CUSTOM_ATTRIBUTE_CLASS.equals(argument1)){
                return classNames.getOrDefault(argument2, null);
            } else if(GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS.equals(argument1)){
                return serializerClassNames.getOrDefault(argument2, null);
            }
            return null;
        });

        when(configuration.has(any(), any())).thenAnswer(invocation -> {
            Object argument1 = invocation.getArguments()[0];
            Object argument2 = invocation.getArguments()[1];
            return GraphDatabaseConfiguration.CUSTOM_SERIALIZER_CLASS.equals(argument1)
                && attributeIds.contains(argument2);
        });
    }

    private AttributeComponents makeAttributeComponents(String idSuffix, String className, String serializerClassName){
        AttributeComponents attributeComponents = new AttributeComponents();
        attributeComponents.attributeId = GraphDatabaseConfiguration.ATTRIBUTE_PREFIX+idSuffix;
        attributeComponents.classname = className;
        attributeComponents.serializerClassName = serializerClassName;
        return attributeComponents;
    }

    private class AttributeComponents {
        private String attributeId;
        private String classname;
        private String serializerClassName;
    }
}

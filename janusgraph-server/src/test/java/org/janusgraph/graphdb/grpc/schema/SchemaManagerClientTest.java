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

package org.janusgraph.graphdb.grpc.schema;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.janusgraph.graphdb.grpc.JanusGraphGrpcServerBaseTest;
import org.janusgraph.graphdb.grpc.JanusGraphManagerClient;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.EdgeProperty;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.PropertyDataType;
import org.janusgraph.graphdb.grpc.types.VertexLabel;
import org.janusgraph.graphdb.grpc.types.VertexProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

public class SchemaManagerClientTest extends JanusGraphGrpcServerBaseTest {

    private static final String defaultGraphName = "graph";

    private JanusGraphContext getDefaultContext() {
        JanusGraphManagerClient janusGraphManagerClient = new JanusGraphManagerClient(managedChannel);
        return janusGraphManagerClient.getContextByGraphName(defaultGraphName);
    }

    @Test
    public void testGetVertexLabelByNameNotFound() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getVertexLabelByName("test"));

        assertEquals(Status.NOT_FOUND.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetVertexLabelByNameInvalidArgumentEmptyName() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getVertexLabelByName(""));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetVertexLabelByNameInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, () -> schemaManagerClient.getVertexLabelByName("test"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetVertexLabelByNameVertexLabelExists(String name) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        long id = createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        assertEquals(name, vertexLabel.getName());
        assertFalse(vertexLabel.getPartitioned());
        assertFalse(vertexLabel.getReadOnly());
        assertEquals(id, vertexLabel.getId().getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetVertexLabelByNameVertexLabelSetReadOnly(boolean readOnly) {
        final String name = "testReadOnly";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name)
            .setReadOnly(readOnly));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        assertEquals(readOnly, vertexLabel.getReadOnly());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetVertexLabelByNameVertexLabelSetPartitioned(boolean partitioned) {
        final String name = "testPartitioned";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name)
            .setPartitioned(partitioned));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        assertEquals(partitioned, vertexLabel.getPartitioned());
    }

    @Test
    public void testGetVertexLabelsInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, schemaManagerClient::getVertexLabels);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 16})
    public void testGetVertexLabels(int numberOfVertices) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);
        List<Long> createdIds = new ArrayList<>();

        for (int i = 0; i < numberOfVertices; i++) {
            long id = createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
                .setName("testMultipleVertices"+i));
            createdIds.add(id);
        }

        List<VertexLabel> vertexLabels = schemaManagerClient.getVertexLabels();

        assertEquals(numberOfVertices, vertexLabels.size());
        for (Long createdId : createdIds) {
            long count = vertexLabels.stream().filter(v -> v.getId().getValue() == createdId).count();
            assertEquals(1, count);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetVertexLabelByNameVertexLabelWithVertexProperty(String propertyName) {
        final String name = "testVertexProperty";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        VertexProperty test = VertexProperty.newBuilder()
            .setName(propertyName)
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
            .setCardinality(VertexProperty.Cardinality.CARDINALITY_SINGLE)
            .build();
        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name)
            .addProperties(test));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        VertexProperty property = vertexLabel.getProperties(0);
        assertEquals(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN, property.getDataType());
        assertEquals(VertexProperty.Cardinality.CARDINALITY_SINGLE, property.getCardinality());
        assertEquals(propertyName, property.getName());
        assertNotEquals(0L, property.getId().getValue());
    }

    @ParameterizedTest
    @EnumSource(value = PropertyDataType.class, mode = EXCLUDE, names = { "PROPERTY_DATA_TYPE_UNSPECIFIED", "UNRECOGNIZED" })
    public void testGetVertexLabelByNameVertexLabelWithVertexProperty(PropertyDataType propertyDataType) {
        final String name = "testVertexPropertyType";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        VertexProperty test = VertexProperty.newBuilder()
            .setName(name)
            .setDataType(propertyDataType)
            .build();
        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name)
            .addProperties(test));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        VertexProperty property = vertexLabel.getProperties(0);
        assertEquals(propertyDataType, property.getDataType());
        assertEquals(name, property.getName());
    }

    @ParameterizedTest
    @EnumSource(value = VertexProperty.Cardinality.class, mode = EXCLUDE, names = { "CARDINALITY_UNSPECIFIED", "UNRECOGNIZED" })
    public void testGetVertexLabelByNameVertexLabelWithVertexProperty(VertexProperty.Cardinality cardinality) {
        final String name = "testPropertyCardinality";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        VertexProperty test = VertexProperty.newBuilder()
            .setName(name)
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
            .setCardinality(cardinality)
            .build();
        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name)
            .addProperties(test));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        VertexProperty property = vertexLabel.getProperties(0);
        assertEquals(cardinality, property.getCardinality());
        assertEquals(name, property.getName());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 16})
    public void testGetVertexLabelByNameVertexLabelWithMultipleVertexProperties(int numberOfProperties) {
        final String name = "testMultipleVertexProperties";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        VertexLabel.Builder builder = VertexLabel.newBuilder()
            .setName(name);

        for (int i = 0; i < numberOfProperties; i++) {
            VertexProperty test = VertexProperty.newBuilder()
                .setName("test"+i)
                .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
                .setCardinality(VertexProperty.Cardinality.CARDINALITY_SINGLE)
                .build();
            builder.addProperties(test);
        }
        createVertexLabel(defaultGraphName, builder);

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        assertEquals(numberOfProperties, vertexLabel.getPropertiesCount());
    }

    @Test
    public void testGetEdgeLabelByNameNotFound() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getEdgeLabelByName("test"));

        assertEquals(Status.NOT_FOUND.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetEdgeLabelByNameInvalidArgumentEmptyName() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getEdgeLabelByName(""));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetEdgeLabelByNameInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, () -> schemaManagerClient.getEdgeLabelByName("test"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetEdgeLabelByNameEdgeLabelExists(String name) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        long id = createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(name);

        assertEquals(name, edgeLabel.getName());
        assertEquals(EdgeLabel.Direction.DIRECTION_BOTH, edgeLabel.getDirection());
        assertEquals(EdgeLabel.Multiplicity.MULTIPLICITY_MULTI, edgeLabel.getMultiplicity());
        assertEquals(id, edgeLabel.getId().getValue());
    }

    @ParameterizedTest
    @EnumSource(value = EdgeLabel.Multiplicity.class, mode = EXCLUDE, names = {"MULTIPLICITY_UNSPECIFIED", "UNRECOGNIZED"})
    public void testGetEdgeLabelByNameWithDefinedMultiplicity(EdgeLabel.Multiplicity multiplicity) {
        final String name = "testMultiplicity";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name)
            .setDirection(EdgeLabel.Direction.DIRECTION_BOTH)
            .setMultiplicity(multiplicity));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(name);

        assertEquals(multiplicity, edgeLabel.getMultiplicity());
    }

    @ParameterizedTest
    @EnumSource(value = EdgeLabel.Direction.class, mode = EXCLUDE, names = {"DIRECTION_UNSPECIFIED", "UNRECOGNIZED"})
    public void testGetEdgeLabelByNameWithDefinedDirection(EdgeLabel.Direction direction) {
        final String name = "testDirection";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name)
            .setDirection(direction));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(name);

        assertEquals(direction, edgeLabel.getDirection());
    }

    @Test
    public void testGetEdgeLabelsInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, schemaManagerClient::getEdgeLabels);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 16})
    public void testGetEdgeLabels(int numberOfEdges) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);
        List<Long> createdIds = new ArrayList<>();

        for (int i = 0; i < numberOfEdges; i++) {
            long id = createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
                .setName("testMultipleEdges"+i));
            createdIds.add(id);
        }

        List<EdgeLabel> edgeLabels = schemaManagerClient.getEdgeLabels();

        assertEquals(numberOfEdges, edgeLabels.size());
        for (Long createdId : createdIds) {
            long count = edgeLabels.stream().filter(e -> e.getId().getValue() == createdId).count();
            assertEquals(1, count);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetEdgeLabelByNameEdgeLabelWithEdgeProperty(String propertyName) {
        final String name = "testEdgeProperty";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        EdgeProperty test = EdgeProperty.newBuilder()
            .setName(propertyName)
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
            .build();
        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name)
            .addProperties(test));

        EdgeLabel label = schemaManagerClient.getEdgeLabelByName(name);

        EdgeProperty property = label.getProperties(0);
        assertEquals(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN, property.getDataType());
        assertEquals(propertyName, property.getName());
        assertNotEquals(0, property.getId().getValue());
    }

    @ParameterizedTest
    @EnumSource(value = PropertyDataType.class, mode = EXCLUDE, names = { "PROPERTY_DATA_TYPE_UNSPECIFIED", "UNRECOGNIZED" })
    public void testGetEdgeLabelByNameEdgeLabelWithEdgeProperty(PropertyDataType propertyDataType) {
        final String name = "testEdgePropertyType";
        final String propertyName =name + "-prop";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        EdgeProperty test = EdgeProperty.newBuilder()
            .setName(propertyName)
            .setDataType(propertyDataType)
            .build();
        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name)
            .addProperties(test));

        EdgeLabel label = schemaManagerClient.getEdgeLabelByName(name);

        EdgeProperty property = label.getProperties(0);
        assertEquals(propertyDataType, property.getDataType());
        assertEquals(propertyName, property.getName());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 16})
    public void testGetEdgeLabelByNameEdgeLabelWithMultipleEdgeProperties(int numberOfProperties) {
        final String name = "testMultipleEdgeProperties";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        EdgeLabel.Builder builder = EdgeLabel.newBuilder()
            .setName(name);

        for (int i = 0; i < numberOfProperties; i++) {
            EdgeProperty test = EdgeProperty.newBuilder()
                .setName("test"+i)
                .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
                .build();
            builder.addProperties(test);
        }
        createEdgeLabel(defaultGraphName, builder);

        EdgeLabel label = schemaManagerClient.getEdgeLabelByName(name);

        assertEquals(numberOfProperties, label.getPropertiesCount());
    }
}

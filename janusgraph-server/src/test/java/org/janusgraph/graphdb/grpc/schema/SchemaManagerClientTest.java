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

import com.google.protobuf.Any;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.janusgraph.graphdb.grpc.JanusGraphGrpcServerBaseTest;
import org.janusgraph.graphdb.grpc.JanusGraphManagerClient;
import org.janusgraph.graphdb.grpc.types.EdgeLabel;
import org.janusgraph.graphdb.grpc.types.EdgeProperty;
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.PropertyDataType;
import org.janusgraph.graphdb.grpc.types.VertexCompositeGraphIndex;
import org.janusgraph.graphdb.grpc.types.VertexLabel;
import org.janusgraph.graphdb.grpc.types.VertexProperty;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
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

    private boolean isIdEqualCreated(Any id, Object createdId) {
        try {
            return id.unpack(Int64Value.class).getValue() == (long) createdId;
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
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
    public void testGetVertexLabelByNameVertexLabelExists(String name) throws InvalidProtocolBufferException {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        Object id = createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(name);

        assertEquals(name, vertexLabel.getName());
        assertFalse(vertexLabel.getPartitioned());
        assertFalse(vertexLabel.getReadOnly());
        assertEquals(id, vertexLabel.getId().unpack(Int64Value.class).getValue());
    }

    @Test
    public void testVertexLabelDoubleCheckId() throws InvalidProtocolBufferException {
        final String name = "testDoubleCheckId";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        Object id = createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(name));

        VertexLabel firstCheck = schemaManagerClient.getVertexLabelByName(name);
        assertEquals(id, firstCheck.getId().unpack(Int64Value.class).getValue());

        VertexLabel secondCheck = schemaManagerClient.getVertexLabelByName(name);
        assertEquals(id, secondCheck.getId().unpack(Int64Value.class).getValue());
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
        List<Object> createdIds = new ArrayList<>();

        for (int i = 0; i < numberOfVertices; i++) {
            Object id = createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
                .setName("testMultipleVertices" + i));
            createdIds.add(id);
        }

        List<VertexLabel> vertexLabels = schemaManagerClient.getVertexLabels();

        assertEquals(numberOfVertices, vertexLabels.size());
        for (Object createdId : createdIds) {
            long count = vertexLabels.stream().filter(v -> isIdEqualCreated(v.getId(), createdId)).count();
            assertEquals(1, count);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetVertexLabelByNameVertexLabelWithVertexProperty(String propertyName) throws InvalidProtocolBufferException {
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
        assertNotEquals(0L, property.getId().unpack(Int64Value.class).getValue());
    }

    @Test
    public void testVertexPropertyDoubleCheckId() {
        final String name = "testDoubleCheckId";
        final String propertyName = "testDoubleCheckIdProperty";
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

        Any firstId = schemaManagerClient.getVertexLabelByName(name).getProperties(0).getId();
        Any secondId = schemaManagerClient.getVertexLabelByName(name).getProperties(0).getId();

        assertEquals(firstId, secondId);
    }

    @ParameterizedTest
    @EnumSource(value = PropertyDataType.class, mode = EXCLUDE, names = {"PROPERTY_DATA_TYPE_UNSPECIFIED", "UNRECOGNIZED"})
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
    @EnumSource(value = VertexProperty.Cardinality.class, mode = EXCLUDE, names = {"CARDINALITY_UNSPECIFIED", "UNRECOGNIZED"})
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
                .setName("test" + i)
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
    public void testGetEdgeLabelByNameEdgeLabelExists(String name) throws InvalidProtocolBufferException {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        Object id = createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(name);

        assertEquals(name, edgeLabel.getName());
        assertEquals(EdgeLabel.Direction.DIRECTION_BOTH, edgeLabel.getDirection());
        assertEquals(EdgeLabel.Multiplicity.MULTIPLICITY_MULTI, edgeLabel.getMultiplicity());
        assertEquals(id, edgeLabel.getId().unpack(Int64Value.class).getValue());
    }

    @Test
    public void testEdgeLabelDoubleCheckId() throws InvalidProtocolBufferException {
        final String name = "testDoubleCheckId";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        Object id = createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name));

        EdgeLabel firstCheck = schemaManagerClient.getEdgeLabelByName(name);
        assertEquals(id, firstCheck.getId().unpack(Int64Value.class).getValue());

        EdgeLabel secondCheck = schemaManagerClient.getEdgeLabelByName(name);
        assertEquals(id, secondCheck.getId().unpack(Int64Value.class).getValue());
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
        List<Object> createdIds = new ArrayList<>();

        for (int i = 0; i < numberOfEdges; i++) {
            Object id = createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
                .setName("testMultipleEdges" + i));
            createdIds.add(id);
        }

        List<EdgeLabel> edgeLabels = schemaManagerClient.getEdgeLabels();

        assertEquals(numberOfEdges, edgeLabels.size());
        for (Object createdId : createdIds) {
            long count = edgeLabels.stream().filter(e -> isIdEqualCreated(e.getId(), createdId)).count();
            assertEquals(1, count);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetEdgeLabelByNameEdgeLabelWithEdgeProperty(String propertyName) throws InvalidProtocolBufferException {
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
        assertNotEquals(0, property.getId().unpack(Int64Value.class).getValue());
    }

    @Test
    public void testEdgePropertyDoubleCheckId() {
        final String name = "testDoubleCheckId";
        final String propertyName = "testDoubleCheckIdProperty";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        EdgeProperty test = EdgeProperty.newBuilder()
            .setName(propertyName)
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
            .build();
        //create vertex
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(name)
            .addProperties(test));

        Any firstId = schemaManagerClient.getEdgeLabelByName(name).getProperties(0).getId();
        Any secondId = schemaManagerClient.getEdgeLabelByName(name).getProperties(0).getId();

        assertEquals(firstId, secondId);
    }

    @ParameterizedTest
    @EnumSource(value = PropertyDataType.class, mode = EXCLUDE, names = {"PROPERTY_DATA_TYPE_UNSPECIFIED", "UNRECOGNIZED"})
    public void testGetEdgeLabelByNameEdgeLabelWithEdgeProperty(PropertyDataType propertyDataType) {
        final String name = "testEdgePropertyType";
        final String propertyName = name + "-prop";
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
                .setName("test" + i)
                .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
                .build();
            builder.addProperties(test);
        }
        createEdgeLabel(defaultGraphName, builder);

        EdgeLabel label = schemaManagerClient.getEdgeLabelByName(name);

        assertEquals(numberOfProperties, label.getPropertiesCount());
    }

    @Test
    public void testGetVertexCompositeGraphIndexByNameNotFound() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getVertexCompositeGraphIndexByName("test"));

        assertEquals(Status.NOT_FOUND.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetVertexCompositeGraphIndexByNameInvalidArgumentEmptyName() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        StatusRuntimeException test = assertThrows(StatusRuntimeException.class, () -> schemaManagerClient.getVertexCompositeGraphIndexByName(""));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), test.getStatus().getCode());
    }

    @Test
    public void testGetVertexCompositeGraphIndexByNameInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, () -> schemaManagerClient.getVertexCompositeGraphIndexByName("test"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetVertexCompositeGraphIndexByNameCompositeGraphIndexExists(String name) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        VertexProperty property = VertexProperty.newBuilder()
            .setName("testProperty")
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_STRING)
            .build();
        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName("testVertex")
            .addProperties(property));

        createVertexCompositeGraphIndex(defaultGraphName, VertexCompositeGraphIndex.newBuilder()
            .setName(name)
            .addKeys(property));

        VertexCompositeGraphIndex index = schemaManagerClient.getVertexCompositeGraphIndexByName(name);

        assertEquals(name, index.getName());
        assertEquals(1, index.getKeysCount());
        assertEquals("testProperty", index.getKeys(0).getName());
        assertFalse(index.getUnique());
        assertFalse(index.hasIndexOnly());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetVertexCompositeGraphIndexByNameCompositeGraphIndexAndUnique(boolean unique) {
        final String name = "testCompositeGraphIndexAndMultipleProperties";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        VertexProperty property = VertexProperty.newBuilder()
            .setName("testProperty")
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_STRING)
            .build();
        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName("testVertex")
            .addProperties(property));

        createVertexCompositeGraphIndex(defaultGraphName, VertexCompositeGraphIndex.newBuilder()
            .setName(name)
            .setUnique(unique)
            .addKeys(property));

        VertexCompositeGraphIndex index = schemaManagerClient.getVertexCompositeGraphIndexByName(name);

        assertEquals(name, index.getName());
        assertEquals(1, index.getKeysCount());
        assertEquals("testProperty", index.getKeys(0).getName());
        assertEquals(unique, index.getUnique());
    }

    @ParameterizedTest
    @ValueSource(strings = {"test", "test2"})
    public void testGetVertexCompositeGraphIndexByNameCompositeGraphIndexExistsAndIndexOnly(String name) throws InvalidProtocolBufferException {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create property
        VertexProperty property = VertexProperty.newBuilder()
            .setName("testProperty")
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_STRING)
            .build();
        //create vertex
        VertexLabel vertex = VertexLabel.newBuilder()
            .setName("testVertex")
            .addProperties(property)
            .build();
        Object vertexLabelId = createVertexLabel(defaultGraphName, vertex);

        createVertexCompositeGraphIndex(defaultGraphName, VertexCompositeGraphIndex.newBuilder()
            .setName(name)
            .setIndexOnly(vertex)
            .addKeys(property));

        VertexCompositeGraphIndex index = schemaManagerClient.getVertexCompositeGraphIndexByName(name);

        assertEquals(name, index.getName());
        assertEquals(1, index.getKeysCount());
        assertEquals("testProperty", index.getKeys(0).getName());
        assertFalse(index.getUnique());
        assertTrue(index.hasIndexOnly());
        assertEquals(vertexLabelId, index.getIndexOnly().getId().unpack(Int64Value.class).getValue());
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 16})
    public void testGetVertexCompositeGraphIndexByNameCompositeGraphIndexAndMultipleProperties(int numberOfProperties) {
        final String name = "testCompositeGraphIndexAndMultipleProperties";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        VertexLabel.Builder builder = VertexLabel.newBuilder()
            .setName(name);

        VertexCompositeGraphIndex.Builder indexBuilder = VertexCompositeGraphIndex.newBuilder()
            .setName(name);

        for (int i = 0; i < numberOfProperties; i++) {
            VertexProperty test = VertexProperty.newBuilder()
                .setName("test" + i)
                .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
                .setCardinality(VertexProperty.Cardinality.CARDINALITY_SINGLE)
                .build();
            builder.addProperties(test);
            indexBuilder.addKeys(test);
        }
        createVertexLabel(defaultGraphName, builder);
        createVertexCompositeGraphIndex(defaultGraphName, indexBuilder);
        VertexCompositeGraphIndex index = schemaManagerClient.getVertexCompositeGraphIndexByName(name);

        assertEquals(name, index.getName());
        assertEquals(numberOfProperties, index.getKeysCount());
    }

    @Test
    public void testGetVertexCompositeGraphIndicesInvalidArgumentNullContext() {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(null, managedChannel);

        assertThrows(NullPointerException.class, schemaManagerClient::getVertexCompositeGraphIndices);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 4, 8, 16})
    public void testGetVertexCompositeGraphIndices(int numberOfVertices) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);
        List<Object> createdIds = new ArrayList<>();
        VertexProperty test = VertexProperty.newBuilder()
            .setName("test")
            .setDataType(PropertyDataType.PROPERTY_DATA_TYPE_BOOLEAN)
            .setCardinality(VertexProperty.Cardinality.CARDINALITY_SINGLE)
            .build();
        VertexLabel vertexLabel = VertexLabel.newBuilder().setName("testMultipleVertices").addProperties(test).build();
        createVertexLabel(defaultGraphName, vertexLabel);

        for (int i = 0; i < numberOfVertices; i++) {
            VertexCompositeGraphIndex.Builder indexBuilder = VertexCompositeGraphIndex.newBuilder()
                .setName("testGetVertexCompositeGraphIndices" + i)
                .addKeys(test);
            Object id = createVertexCompositeGraphIndex(defaultGraphName, indexBuilder);
                createdIds.add(id);
        }

        List<VertexCompositeGraphIndex> compositeGraphIndices = schemaManagerClient.getVertexCompositeGraphIndices();

        assertEquals(numberOfVertices, compositeGraphIndices.size());
        for (Object createdId : createdIds) {
            long count = compositeGraphIndices.stream().filter(v -> isIdEqualCreated(v.getId(), createdId)).count();
            assertEquals(1, count);
        }
    }
}

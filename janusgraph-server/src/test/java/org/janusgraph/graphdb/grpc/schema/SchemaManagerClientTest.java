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
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.VertexLabel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    public void testGetVertexLabelByNameVertexLabelExists(String vertexLabelName) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        long id = createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(vertexLabelName));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertEquals(vertexLabelName, vertexLabel.getName());
        assertFalse(vertexLabel.getPartitioned());
        assertFalse(vertexLabel.getReadOnly());
        assertEquals(id, vertexLabel.getId().getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetVertexLabelByNameVertexLabelSetReadOnly(boolean readOnly) {
        final String vertexLabelName = "testReadOnly";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(vertexLabelName)
            .setReadOnly(readOnly));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertEquals(readOnly, vertexLabel.getReadOnly());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testGetVertexLabelByNameVertexLabelSetPartitioned(boolean partitioned) {
        final String vertexLabelName = "testPartitioned";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder()
            .setName(vertexLabelName)
            .setPartitioned(partitioned));

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

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
    public void testGetEdgeLabelByNameEdgeLabelExists(String edgeLabelName) {
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        long id = createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(edgeLabelName));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(edgeLabelName);

        assertEquals(edgeLabelName, edgeLabel.getName());
        assertEquals(EdgeLabel.Direction.BOTH, edgeLabel.getDirection());
        assertEquals(EdgeLabel.Multiplicity.MULTI, edgeLabel.getMultiplicity());
        assertEquals(id, edgeLabel.getId().getValue());
    }

    @ParameterizedTest
    @EnumSource(mode = EXCLUDE, names = {"UNRECOGNIZED"})
    public void testGetEdgeLabelByNameWithDefinedMultiplicity(EdgeLabel.Multiplicity multiplicity) {
        final String edgeLabelName = "testMultiplicity";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(edgeLabelName)
            .setDirection(EdgeLabel.Direction.BOTH)
            .setMultiplicity(multiplicity));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(edgeLabelName);

        assertEquals(multiplicity, edgeLabel.getMultiplicity());
    }

    @ParameterizedTest
    @EnumSource(mode = EXCLUDE, names = {"UNRECOGNIZED"})
    public void testGetEdgeLabelByNameWithDefinedDirection(EdgeLabel.Direction direction) {
        final String edgeLabelName = "testDirection";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create edge
        createEdgeLabel(defaultGraphName, EdgeLabel.newBuilder()
            .setName(edgeLabelName)
            .setDirection(direction));

        EdgeLabel edgeLabel = schemaManagerClient.getEdgeLabelByName(edgeLabelName);

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
}

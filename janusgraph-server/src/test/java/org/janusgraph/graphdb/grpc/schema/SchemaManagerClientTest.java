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
import org.janusgraph.graphdb.grpc.types.JanusGraphContext;
import org.janusgraph.graphdb.grpc.types.VertexLabel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @Test
    public void testGetVertexLabelByNameVertexLabelExists() {
        final String vertexLabelName = "test";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder().setName(vertexLabelName).build());

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertEquals(vertexLabelName, vertexLabel.getName());
        assertFalse(vertexLabel.getPartitioned());
        assertFalse(vertexLabel.getReadOnly());
        assertNotEquals("", vertexLabel.getId());
    }

    @Test
    public void testGetVertexLabelByNameVertexLabelIsReadOnly() {
        final String vertexLabelName = "testReadOnly";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder().setName(vertexLabelName).setReadOnly(true).build());

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertEquals(vertexLabelName, vertexLabel.getName());
        assertFalse(vertexLabel.getPartitioned());
        assertTrue(vertexLabel.getReadOnly());
        assertNotEquals("", vertexLabel.getId());
    }

    @Test
    public void testGetVertexLabelByNameVertexLabelIsPartitioned() {
        final String vertexLabelName = "testPartitioned";
        SchemaManagerClient schemaManagerClient = new SchemaManagerClient(getDefaultContext(), managedChannel);

        //create vertex
        createVertexLabel(defaultGraphName, VertexLabel.newBuilder().setName(vertexLabelName).setPartitioned(true).build());

        VertexLabel vertexLabel = schemaManagerClient.getVertexLabelByName(vertexLabelName);

        assertEquals(vertexLabelName, vertexLabel.getName());
        assertTrue(vertexLabel.getPartitioned());
        assertFalse(vertexLabel.getReadOnly());
        assertNotEquals("", vertexLabel.getId());
    }
}
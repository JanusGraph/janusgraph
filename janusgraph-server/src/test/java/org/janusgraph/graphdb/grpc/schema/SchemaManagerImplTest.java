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
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SchemaManagerImplTest extends JanusGraphGrpcServerBaseTest {

    @Test
    public void testGetVertexLabelByNameContextIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getVertexLabelByName(GetVertexLabelByNameRequest.newBuilder().build()));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void testGetVertexLabelByNameRequestIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getVertexLabelByName(null));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void testGetVertexLabelsContextIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getVertexLabels(GetVertexLabelsRequest.newBuilder().build()));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void testGetVertexLabelsRequestIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getVertexLabels(null));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }


    @Test
    public void testGetEdgeLabelByNameContextIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getEdgeLabelByName(GetEdgeLabelByNameRequest.newBuilder().build()));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void testGetEdgeLabelByNameRequestIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getEdgeLabelByName(null));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void testGetEdgeLabelsContextIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getEdgeLabels(GetEdgeLabelsRequest.newBuilder().build()));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }

    @Test
    public void testGetEdgeLabelsRequestIsNull() {
        SchemaManagerServiceGrpc.SchemaManagerServiceBlockingStub stub = SchemaManagerServiceGrpc.newBlockingStub(managedChannel);

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> stub.getEdgeLabels(null));

        assertEquals(Status.INVALID_ARGUMENT.getCode(), exception.getStatus().getCode());
    }
}

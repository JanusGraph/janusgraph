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

package org.janusgraph.core;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.GraphSONMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper;
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class JanusGraphContainer extends GenericContainer<JanusGraphContainer> {

    public static final Integer GREMLIN_PORT = 8182;

    public JanusGraphContainer() {
        super("janusgraph/janusgraph:latest");
        addExposedPort(GREMLIN_PORT);
        waitingFor(Wait.forListeningPort());
    }

    public RemoteConnection remoteConnectionWithGraphSONV3d0() {
        Cluster cluster = Cluster.build(getContainerIpAddress())
            .port(getMappedPort(GREMLIN_PORT))
            .serializer(new GraphSONMessageSerializerV3d0(GraphSONMapper.build().addRegistry(JanusGraphIoRegistry.instance())))
            .create();
        return DriverRemoteConnection.using(cluster, "g");
    }

    public RemoteConnection remoteConnectionWithGryo() {
        Cluster cluster = Cluster.build(getContainerIpAddress())
            .port(getMappedPort(GREMLIN_PORT))
            .serializer(new GryoMessageSerializerV3d0(GryoMapper.build().addRegistry(JanusGraphIoRegistry.instance())))
            .create();
        return DriverRemoteConnection.using(cluster, "g");
    }
}

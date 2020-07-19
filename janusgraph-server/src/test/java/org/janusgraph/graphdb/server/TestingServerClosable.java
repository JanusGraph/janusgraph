// Copyright 2020 JanusGraph Authors
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

package org.janusgraph.graphdb.server;

import io.grpc.ManagedChannel;
import io.grpc.Server;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class TestingServerClosable implements Closeable {
    private final Server server;
    private final ManagedChannel channel;

    public TestingServerClosable(Server server, ManagedChannel channel){
        this.server = server;
        this.channel = channel;
    }

    @Override
    public void close() throws IOException {
        channel.shutdown();
        server.shutdown();
        try{
            channel.awaitTermination(1, TimeUnit.SECONDS);
            server.awaitTermination();
        }catch (InterruptedException e){
            throw new IOException(e);
        }
    }
}

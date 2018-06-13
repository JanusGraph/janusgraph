#region License

/*
 * Copyright 2018 JanusGraph Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#endregion

using System;
using System.Collections.Generic;
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Remote;
using Gremlin.Net.Structure.IO.GraphSON;
using JanusGraph.Net.Extensions.Geoshapes;
using JanusGraph.Net.Extensions.IO.GraphSON;

namespace JanusGraph.Net.Extensions.IntegrationTest
{
    internal class RemoteConnectionFactory : IDisposable
    {
        private static readonly string Host = ConfigProvider.Configuration["Host"];
        private static readonly int Port = Convert.ToInt32(ConfigProvider.Configuration["Port"]);

        private readonly IList<DriverRemoteConnection> _connections = new List<DriverRemoteConnection>();

        public IRemoteConnection CreateRemoteConnection(string traversalSource = "gods_traversal")
        {
            var c = new DriverRemoteConnection(
                JanusGraphClientBuilder.BuildClientForServer(new GremlinServer(Host, Port)).Create(),
                traversalSource);
            _connections.Add(c);
            return c;
        }

        public void Dispose()
        {
            foreach (var connection in _connections)
            {
                connection.Dispose();
            }
        }
    }
}
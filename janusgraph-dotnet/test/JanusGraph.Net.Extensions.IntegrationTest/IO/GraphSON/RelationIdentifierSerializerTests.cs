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

using System.Threading.Tasks;
using Gremlin.Net.Structure;
using Xunit;

namespace JanusGraph.Net.Extensions.IntegrationTest.IO.GraphSON
{
    public class RelationIdentifierSerializerTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();
        
        [Fact]
        public async Task TraversalWithRelationIdentifierAsEdgeId_ExistingEdgeId_EdgeFound()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());
            var edgeId = await g.E().Id().Promise(t => t.Next());
            
            var count = await g.E(edgeId).Count().Promise(t => t.Next());

            Assert.Equal(1, count);
        }
        
        [Fact]
        public async Task TraversalWithEdge_ExistingEdge_EdgeFound()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());
            var edge = await g.E().Promise(t => t.Next());

            var count = await g.E(edge).Count().Promise(t => t.Next());

            Assert.Equal(1, count);
        }
    }
}
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
using JanusGraph.Net.Extensions.Geoshapes;
using Xunit;

namespace JanusGraph.Net.Extensions.IntegrationTest.IO.GraphSON
{
    public class GeoshapeDeserializerTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();

        [Fact]
        public async Task TraversalWithPointPropertyValue_PointReturned_ExpectedPoint()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var place = await g.V().Has("demigod", "name", "hercules").OutE("battled").Has("time", 1)
                .Values<Point>("place").Promise(t => t.Next());

            var expectedPlace = Geoshape.Point(38.1f, 23.7f);
            Assert.Equal(expectedPlace.Latitude, place.Latitude, 3);
            Assert.Equal(expectedPlace.Longitude, place.Longitude, 3);
        }
    }
}
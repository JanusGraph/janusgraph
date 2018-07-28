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
using Gremlin.Net.Driver;
using Gremlin.Net.Driver.Remote;
using Gremlin.Net.Process.Traversal;
using Gremlin.Net.Structure;
using Gremlin.Net.Structure.IO.GraphSON;
using JanusGraph.Net.Extensions.Geoshapes;
using Xunit;

namespace JanusGraph.Net.Extensions.IntegrationTest
{
    public class DocTraversalsTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();
        
        [Fact]
        public void GremlinNetGettingStartedTest()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var herculesAge = g.V().Has("name", "hercules").Values<int>("age").Next();

            Assert.Equal(30, herculesAge);
        }

        [Fact]
        public void ReceivingEdgesTest()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());
            
            var edges = g.V().Has("name","hercules").OutE("battled").ToList();
            
            Assert.Equal(3, edges.Count);
        }

        [Fact]
        public void TextContainsPredicateTest()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());
            
            var reasons = g.E().Has("reason", Text.TextContains("loves")).ToList();
            
            Assert.Equal(2, reasons.Count);
        }

        [Fact]
        public void GeoTypesPointsReceivedTest()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());
            
            var firstBattlePlace = g.V().Has("name", "hercules").OutE("battled").Order().By("time")
                .Values<Point>("place").Next();
            
            Assert.Equal(38.1f, firstBattlePlace.Latitude, 3);
            Assert.Equal(23.7f, firstBattlePlace.Longitude, 3);
        }

        [Fact]
        public void GeoTypesPointAsArgumentTest()
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());
            
            g.V().Has("name", "hercules").OutE("battled").Has("place", Geoshape.Point(38.1f, 23.7f)).Next();
        }
    }
}
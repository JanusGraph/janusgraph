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
using static JanusGraph.Net.Extensions.Text;

namespace JanusGraph.Net.Extensions.IntegrationTest
{
    public class TextTests
    {
        private readonly RemoteConnectionFactory _connectionFactory = new RemoteConnectionFactory();       
        
        [Theory]
        [InlineData("loves", 2)]
        [InlineData("shouldNotBeFound", 0)]
        public async Task TextContains_GivenSearchText_ExpectedCountOfElements(string searchText, int expectedCount)
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var count = await g.E().Has("reason", TextContains(searchText)).Count().Promise(t => t.Next());

            Assert.Equal(expectedCount, count);
        }
        
        [Theory]
        [InlineData("wave", 1)]
        [InlineData("f", 2)]
        [InlineData("shouldNotBeFound", 0)]
        public async Task TextContainsPrefix_GivenSearchText_ExpectedCountOfElements(string searchText, int expectedCount)
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var count = await g.E().Has("reason", TextContainsPrefix(searchText)).Count().Promise(t => t.Next());

            Assert.Equal(expectedCount, count);
        }
        
        [Theory]
        [InlineData(".*ave.*", 1)]
        [InlineData("f.{3,4}", 2)]
        [InlineData("shouldNotBeFound", 0)]
        public async Task TextContainsRegex_GivenRegex_ExpectedCountOfElements(string regex, int expectedCount)
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var count = await g.E().Has("reason", TextContainsRegex(regex)).Count().Promise(t => t.Next());

            Assert.Equal(expectedCount, count);
        }
        
        [Theory]
        [InlineData("waxes", 1)]
        [InlineData("shouldNotBeFound", 0)]
        public async Task TextContainsFuzzy_GivenSearchText_ExpectedCountOfElements(string searchText, int expectedCount)
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var count = await g.E().Has("reason", TextContainsFuzzy(searchText)).Count().Promise(t => t.Next());

            Assert.Equal(expectedCount, count);
        }
        
        [Theory]
        [InlineData("herc", 1)]
        [InlineData("s", 3)]
        [InlineData("shouldNotBeFound", 0)]
        public async Task TextPrefix_GivenSearchText_ExpectedCountOfElements(string searchText, int expectedCount)
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var count = await g.V().Has("name", TextPrefix(searchText)).Count().Promise(t => t.Next());

            Assert.Equal(expectedCount, count);
        }
        
        [Theory]
        [InlineData(".*rcule.*", 1)]
        [InlineData("s.{2}", 2)]
        [InlineData("shouldNotBeFound", 0)]
        public async Task TextRegex_GivenRegex_ExpectedCountOfElements(string regex, int expectedCount)
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var count = await g.V().Has("name", TextRegex(regex)).Count().Promise(t => t.Next());

            Assert.Equal(expectedCount, count);
        }
        
        [Theory]
        [InlineData("herculex", 1)]
        [InlineData("ska", 2)]
        [InlineData("shouldNotBeFound", 0)]
        public async Task TextFuzzy_GivenSearchText_ExpectedCountOfElements(string searchText, int expectedCount)
        {
            var g = new Graph().Traversal().WithRemote(_connectionFactory.CreateRemoteConnection());

            var count = await g.V().Has("name", TextFuzzy(searchText)).Count().Promise(t => t.Next());

            Assert.Equal(expectedCount, count);
        }
    }
}

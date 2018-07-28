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

using JanusGraph.Net.Extensions.IO.GraphSON;
using Newtonsoft.Json.Linq;
using Xunit;

namespace JanusGraph.Net.Extensions.UnitTest.IO.GraphSON
{
    public class RelationIdentifierDeserializerTests
    {
        [Fact]
        public void Read_GraphSonRelationIdentifier_ExpectedRelationIdentifier()
        {
            const string graphSon =
                "{\"@type\":\"janusgraph:RelationIdentifier\",\"@value\":{\"relationId\":\"3k1-360-6c5-39c\"}}";
            var reader = GraphSONReaderBuilder.Build().Create();

            var readRelationIdentifier = reader.ToObject(JToken.Parse(graphSon));

            var expectedRelationIdentifier = new RelationIdentifier("3k1-360-6c5-39c");
            Assert.Equal(expectedRelationIdentifier, readRelationIdentifier);
        }
    }
}
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

using System.Globalization;
using JanusGraph.Net.Extensions.Geoshapes;
using JanusGraph.Net.Extensions.IO.GraphSON;
using Xunit;

namespace JanusGraph.Net.Extensions.UnitTest.IO.GraphSON
{
    public class PointSerializerTests
    {
        [Theory]
        [InlineData(-88.8888, -177.1234)]
        [InlineData(87.654321, 156.89)]
        public void Write_PointWithGivenCoordinates_ExpectedGraphSon(double latitude, double longitude)
        {
            var point = Geoshape.Point(latitude, longitude);
            var writer = GraphSONWriterBuilder.Build().Create();

            var graphSon = writer.WriteObject(point);

            var expectedGraphSon = "{\"@type\":\"janusgraph:Geoshape\",\"@value\":{\"coordinates\":[" +
                                   longitude.ToString(CultureInfo.InvariantCulture) + "," +
                                   latitude.ToString(CultureInfo.InvariantCulture) + "]}}";
            Assert.Equal(expectedGraphSon, graphSon);
        }
    }
}

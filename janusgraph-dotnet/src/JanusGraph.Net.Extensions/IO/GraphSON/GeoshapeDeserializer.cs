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
using System.Linq;
using Gremlin.Net.Structure.IO.GraphSON;
using JanusGraph.Net.Extensions.Geoshapes;
using Newtonsoft.Json.Linq;

namespace JanusGraph.Net.Extensions.IO.GraphSON
{
    internal class GeoshapeDeserializer : IGraphSONDeserializer
    {
        public dynamic Objectify(JToken graphsonObject, GraphSONReader reader)
        {
            if (graphsonObject["coordinates"] != null)
                return DeserializePointFromCoordinates(graphsonObject["coordinates"]);
            var geometryData = graphsonObject["geometry"];
            throw new InvalidOperationException($"Deserialization of type {geometryData["type"]} is not supported.");
        }

        private static Point DeserializePointFromCoordinates(JToken coordinates)
        {
            var coordArr = coordinates.Select(c => c.ToObject<double>()).ToArray();
            return PointFromCoordinates(coordArr);
        }

        private static Point PointFromCoordinates(IReadOnlyList<double> coordinates)
        {
            return new Point(coordinates[1], coordinates[0]);
        }
    }
}
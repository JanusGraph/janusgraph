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

using System.Collections.Generic;
using Gremlin.Net.Structure.IO.GraphSON;

namespace JanusGraph.Net.Extensions.IO.GraphSON
{
    internal class RelationIdentifierSerializer : IGraphSONSerializer
    {
        public Dictionary<string, dynamic> Dictify(dynamic objectData, GraphSONWriter writer)
        {
            var relationIdentifier = (RelationIdentifier) objectData;
            return GraphSONUtil.ToTypedValue("RelationIdentifier", ValueDict(relationIdentifier), "janusgraph");
        }

        private static Dictionary<string, object> ValueDict(RelationIdentifier relationIdentifier)
        {
            return new Dictionary<string, object> {{"relationId", relationIdentifier.RelationId}};
        }
    }
}
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
    /// <summary>
    ///     Creates a <see cref="GraphSONReader" /> with the default JanusGraph deserializers and allows adding of custom
    ///     deserializers.
    /// </summary>
    public class GraphSONReaderBuilder
    {
        private readonly Dictionary<string, IGraphSONDeserializer> _deserializerByGraphSONType =
            new Dictionary<string, IGraphSONDeserializer>
            {
                {"janusgraph:Geoshape", new GeoshapeDeserializer()},
                {"janusgraph:RelationIdentifier", new RelationIdentifierDeserializer()}
            };

        private GraphSONReaderBuilder()
        {
        }

        /// <summary>
        ///     Initializes a <see cref="GraphSONReaderBuilder" />.
        /// </summary>
        public static GraphSONReaderBuilder Build()
        {
            return new GraphSONReaderBuilder();
        }

        /// <summary>
        ///     Registers a custom GraphSON deserializer for the given GraphSON type.
        /// </summary>
        /// <param name="graphSONType">The GraphSON type the deserializer should be registered for.</param>
        /// <param name="deserializer">The deserializer to register.</param>
        public GraphSONReaderBuilder RegisterDeserializer(string graphSONType,
            IGraphSONDeserializer deserializer)
        {
            _deserializerByGraphSONType[graphSONType] = deserializer;
            return this;
        }

        /// <summary>
        ///     Creates the <see cref="GraphSONReader" /> with the registered deserializers as well as the default JanusGraph
        ///     deserializers.
        /// </summary>
        public GraphSONReader Create()
        {
            return new GraphSON2Reader(_deserializerByGraphSONType);
        }
    }
}
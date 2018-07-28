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
using Gremlin.Net.Structure.IO.GraphSON;
using JanusGraph.Net.Extensions.Geoshapes;

namespace JanusGraph.Net.Extensions.IO.GraphSON
{
    /// <summary>
    ///     Creates a <see cref="GraphSONWriter" /> with the default JanusGraph serializers and allows adding of custom
    ///     serializers.
    /// </summary>
    public class GraphSONWriterBuilder
    {
        private readonly Dictionary<Type, IGraphSONSerializer> _serializerByType =
            new Dictionary<Type, IGraphSONSerializer>
            {
                {typeof(Point), new PointSerializer()},
                {typeof(RelationIdentifier), new RelationIdentifierSerializer()}
            };

        private GraphSONWriterBuilder()
        {
        }
        
        /// <summary>
        ///     Initializes a <see cref="GraphSONWriterBuilder" />.
        /// </summary>
        public static GraphSONWriterBuilder Build()
        {
            return new GraphSONWriterBuilder();
        }
        
        /// <summary>
        ///     Registers a custom GraphSON serializer for the given type.
        /// </summary>
        /// <param name="type">The type the serializer should be registered for.</param>
        /// <param name="serializer">The serializer to register.</param>
        public GraphSONWriterBuilder RegisterSerializer(Type type, IGraphSONSerializer serializer)
        {
            _serializerByType[type] = serializer;
            return this;
        }

        /// <summary>
        ///     Creates the <see cref="GraphSONWriter" /> with the registered serializers as well as the default JanusGraph
        ///     serializers.
        /// </summary>
        public GraphSONWriter Create()
        {
            return new GraphSON2Writer(_serializerByType);
        }
    }
}
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

namespace JanusGraph.Net.Extensions
{
    /// <summary>
    ///     Identifies an edge.
    /// </summary>
    public class RelationIdentifier : IEquatable<RelationIdentifier>
    {
        /// <summary>
        ///     Initializes a new instance of the <see cref="RelationIdentifier" /> class.
        /// </summary>
        /// <param name="relationId">The underlying relation id.</param>
        public RelationIdentifier(string relationId)
        {
            RelationId = relationId;
        }

        /// <summary>
        ///     Gets the underlying relation id.
        /// </summary>
        public string RelationId { get; }

        /// <inheritdoc />
        public bool Equals(RelationIdentifier other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return string.Equals(RelationId, other.RelationId);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((RelationIdentifier) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return (RelationId != null ? RelationId.GetHashCode() : 0);
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return RelationId;
        }
    }
}
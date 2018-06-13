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

namespace JanusGraph.Net.Extensions.Geoshapes
{
    /// <summary>
    ///     A single point representation. A point is identified by its coordinate on the earth sphere using the spherical
    ///     system of latitudes and longitudes.
    /// </summary>
    public class Point : IEquatable<Point>
    {
        internal Point(double latitude, double longitude)
        {
            Latitude = latitude;
            Longitude = longitude;
        }

        /// <summary>
        ///     Gets the latitude of this point.
        /// </summary>
        public double Latitude { get; }

        /// <summary>
        ///     Gets the longitude of this point.
        /// </summary>
        public double Longitude { get; }

        /// <inheritdoc />
        public bool Equals(Point other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Latitude.Equals(other.Latitude) && Longitude.Equals(other.Longitude);
        }

        /// <inheritdoc />
        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Point) obj);
        }

        /// <inheritdoc />
        public override int GetHashCode()
        {
            unchecked
            {
                return (Latitude.GetHashCode() * 397) ^ Longitude.GetHashCode();
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            return $"POINT ({Longitude} {Latitude})";
        }
    }
}
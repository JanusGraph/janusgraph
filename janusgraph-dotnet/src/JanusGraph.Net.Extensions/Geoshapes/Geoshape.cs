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
    ///     Allows creating of various Geoshapes.
    /// </summary>
    public class Geoshape
    {
        /// <summary>
        ///     Creates <see cref="Point" /> with the given coordinates.
        /// </summary>
        /// <param name="latitude">The latitude of the point.</param>
        /// <param name="longitude">The longitude of the point.</param>
        /// <returns></returns>
        public static Point Point(double latitude, double longitude)
        {
            ThrowIfCoordinatesInvalid(latitude, longitude);
            return new Point(latitude, longitude);
        }

        private static void ThrowIfCoordinatesInvalid(double latitude, double longitude)
        {
            if (AreCoordinatesInvalid(latitude, longitude))
                throw new ArgumentException("Invalid coordinate(s) provided.");
        }

        private static bool AreCoordinatesInvalid(double latitude, double longitude)
        {
            return latitude > 90 || latitude < -90 || longitude > 180 || longitude < -180;
        }
    }
}
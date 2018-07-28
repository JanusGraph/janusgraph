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
using JanusGraph.Net.Extensions.Geoshapes;
using Xunit;

namespace JanusGraph.Net.Extensions.UnitTest.Geoshapes
{
    public class GeoshapeTests
    {
        [Theory]
        [InlineData(90.00001, 0)]
        [InlineData(120.12345, 0)]
        [InlineData(-90.00001, 0)]
        [InlineData(-120.12345, 0)]
        [InlineData(0, 180.000001)]
        [InlineData(0, 200.12345)]
        [InlineData(0, -180.000001)]
        [InlineData(0, -200.12345)]
        public void Point_InvalidCoordinates_ThrowsArgumentException(double latitude, double longitude)
        {
            Assert.Throws<ArgumentException>(() => Geoshape.Point(latitude, longitude));
        }
        
        [Theory]
        [InlineData(0, 0)]
        [InlineData(90, 0)]
        [InlineData(-90, 0)]
        [InlineData(0, 180)]
        [InlineData(0, -180)]
        [InlineData(45.678, 150.9876)]
        [InlineData(-45.678, 150.9876)]
        [InlineData(45.678, -150.9876)]
        [InlineData(-45.678, -150.9876)]
        [InlineData(90, 180)]
        [InlineData(-90, -180)]
        public void Point_ValidCoordinates_PointWithGivenCoordinates(double latitude, double longitude)
        {
            var point = Geoshape.Point(latitude, longitude);
            
            Assert.Equal(latitude, point.Latitude);
            Assert.Equal(longitude, point.Longitude);
        }
    }
}
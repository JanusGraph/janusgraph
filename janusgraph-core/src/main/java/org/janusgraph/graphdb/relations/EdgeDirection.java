// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.relations;

import org.apache.tinkerpop.gremlin.structure.Direction;

/**
 * IMPORTANT: The byte values of the proper directions must be sequential,
 * i.e. the byte values of proper and improper directions may NOT be mixed.
 * This is crucial IN the retrieval for proper edges where we make this assumption.
 *
 * @author Matthias Broecheler (me@matthiasb.com);
 */
public class EdgeDirection {
    public static final Direction[] PROPER_DIRS = {Direction.IN, Direction.OUT};

    public static boolean impliedBy(Direction sub, Direction sup) {
        return sup==sub || sup==Direction.BOTH;
    }

    public static Direction fromPosition(int pos) {
        switch (pos) {
            case 0:
                return Direction.OUT;

            case 1:
                return Direction.IN;

            default:
                throw new IllegalArgumentException("Invalid position:" + pos);
        }
    }

    public static int position(Direction dir) {
        switch (dir) {
            case OUT:
                return 0;

            case IN:
                return 1;

            default:
                throw new IllegalArgumentException("Invalid direction: " + dir);
        }
    }

    public static boolean isProperDirection(Direction dir) {
        return dir==Direction.IN || dir==Direction.OUT;
    }
}

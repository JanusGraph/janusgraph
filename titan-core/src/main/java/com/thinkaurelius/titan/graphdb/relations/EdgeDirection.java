package com.thinkaurelius.titan.graphdb.relations;

import com.tinkerpop.blueprints.Direction;

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

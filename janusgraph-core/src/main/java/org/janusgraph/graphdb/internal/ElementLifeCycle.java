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

package org.janusgraph.graphdb.internal;

import com.google.common.base.Preconditions;

/**
 * ElementLifeCycle enumerates all possible states of the lifecycle of a entity.
 *
 * @author Matthias Broecheler (me@matthiasb.com);
 */
public class ElementLifeCycle {

    public enum Event {REMOVED, REMOVED_RELATION, ADDED_RELATION, UPDATE }

    /**
     * The entity has been newly created and not yet persisted.
     */
    public static final byte New = 1;

    /**
     * The entity has been loaded from the database and has not changed
     * after initial loading.
     */
    public static final byte Loaded = 2;

    /**
     * The entity has changed after being loaded from the database by adding relations.
     */
    private static final byte AddedRelations = 3;

    /**
     * The entity has changed after being loaded from the database by deleting relations.
     */
    private static final byte RemovedRelations = 4;

    /**
     * The entity has changed after being loaded from the database by adding and/or deleting relations.
     */
    private static final byte Modified = 5;

    /**
     * The entity has been deleted but not yet erased from the database.
     */
    public static final byte Removed = 6;


    public static boolean isModified(byte lifecycle) {
        return lifecycle>=AddedRelations && lifecycle<=Modified;
    }

    public static boolean hasRemovedRelations(byte lifecycle) {
        return lifecycle== RemovedRelations || lifecycle==Modified;
    }

    public static boolean hasAddedRelations(byte lifecycle) {
        return lifecycle==AddedRelations || lifecycle==Modified;
    }


    public static boolean isNew(byte lifecycle) {
        return lifecycle==New;
    }

    public static boolean isLoaded(byte lifecycle) {
        return lifecycle==Loaded;
    }

    public static boolean isRemoved(byte lifecycle) {
        return lifecycle== Removed;
    }

    public static boolean isValid(byte lifecycle) {
        return lifecycle>=New && lifecycle<=Removed;
    }

    public static byte update(final byte lifecycle, final Event event) {
        Preconditions.checkArgument(isValid(lifecycle),"Invalid element state: %d", lifecycle);
        if (event==Event.REMOVED) return Removed;
        else if (lifecycle==New || lifecycle==Modified) {
            return lifecycle;
        } else if (lifecycle== Removed) {
            throw new IllegalStateException("No event can occur on deleted vertices: " + event);
        } else if (event==Event.REMOVED_RELATION) {
            switch (lifecycle) {
                case Loaded:
                    return RemovedRelations;
                case AddedRelations:
                    return Modified;
                case RemovedRelations:
                    return RemovedRelations;
                default:
                    throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
            }
        } else if (event==Event.ADDED_RELATION) {
            switch (lifecycle) {
                case Loaded:
                    return AddedRelations;
                case RemovedRelations:
                    return Modified;
                case AddedRelations:
                    return AddedRelations;
                default:
                    throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
            }
        } else if (event==Event.UPDATE) {
            return Modified;
        } else throw new IllegalStateException("Unexpected state event: " + lifecycle + " - " + event);
    }


}

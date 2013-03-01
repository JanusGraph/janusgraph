package com.thinkaurelius.titan.graphdb.internal;

import com.google.common.base.Preconditions;

/**
 * ElementLifeCycle enumerates all possible states of the lifecycle of a entity.
 *
 * @author Matthias Broecheler (me@matthiasb.com);
 */
public class ElementLifeCycle {

    public enum Event {REMOVED, REMOVED_RELATION, ADDED_RELATION }

    /**
     * The entity has been newly created and not yet persisted.
     */
    public final static byte New = 1;

    /**
     * The entity has been loaded from the database and has not changed
     * after initial loading.
     */
    public final static byte Loaded = 2;

    /**
     * The entity has changed after being loaded from the database by adding relations.
     */
    private final static byte AddedRelations = 3;

    /**
     * The entity has changed after being loaded from the database by deleting relations.
     */
    private final static byte RemovedRelations = 4;

    /**
     * The entity has changed after being loaded from the database by adding and/or deleting relations.
     */
    private final static byte Modified = 5;

    /**
     * The entity has been deleted but not yet erased from the database.
     */
    public final static byte Removed = 6;


    public static final boolean isModified(byte lifecycle) {
        return lifecycle>=AddedRelations && lifecycle<=Modified;
    }

    public static final boolean hasRemovedRelations(byte lifecycle) {
        return lifecycle== RemovedRelations || lifecycle==Modified;
    }

    public static final boolean hasAddedRelations(byte lifecycle) {
        return lifecycle==AddedRelations || lifecycle==Modified;
    }


    public static final boolean isNew(byte lifecycle) {
        return lifecycle==New;
    }

    public static final boolean isLoaded(byte lifecycle) {
        return lifecycle==Loaded;
    }

    public static final boolean isRemoved(byte lifecycle) {
        return lifecycle== Removed;
    }




    public static final byte update(final byte lifecycle, final Event event) {
        Preconditions.checkArgument(lifecycle>=New && lifecycle<=Removed,"Invalid element state: " + lifecycle);
        if (event==Event.REMOVED) return Removed;
        else if (lifecycle==New || lifecycle==Modified) {
            return lifecycle;
        } else if (lifecycle== Removed) {
            throw new IllegalStateException("No event can occur on deleted vertices: " + event);
        } else if (event==Event.REMOVED_RELATION) {
            if (lifecycle==Loaded) return RemovedRelations;
            else if (lifecycle==AddedRelations) return Modified;
            else if (lifecycle== RemovedRelations) return RemovedRelations;
            else throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
        } else if (event==Event.ADDED_RELATION) {
            if (lifecycle==Loaded) return AddedRelations;
            else if (lifecycle== RemovedRelations) return Modified;
            else if (lifecycle==AddedRelations) return AddedRelations;
            else throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
        } else throw new IllegalStateException("Unexpected state event: " + lifecycle + " - " + event);
    }


}

package com.thinkaurelius.titan.graphdb.internal;

/**
 * ElementLifeCycle enumerates all possible states of the lifecycle of a entity.
 *
 * @author Matthias Broecheler (me@matthiasb.com);
 */
public class ElementLifeCycle {

    public enum Event {REMOVED, DELETED_RELATION, ADDED_RELATION }

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
    final static byte AddedRelations = 3;

    /**
     * The entity has changed after being loaded from the database by deleting relations.
     */
    final static byte DeletedRelations = 4;

    /**
     * The entity has changed after being loaded from the database by adding and/or deleting relations.
     */
    final static byte Modified = 5;


    /**
     * The entity has been deleted but not yet erased from the database.
     */
    final static byte Removed = 10;


    public static final boolean isModified(byte lifecycle) {
        return lifecycle>=AddedRelations && lifecycle<=Modified;
    }

    public static final boolean hasDeletedRelations(byte lifecycle) {
        return lifecycle==DeletedRelations || lifecycle==Modified;
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
        if (event==Event.REMOVED) return Removed;
        else if (lifecycle==New) return lifecycle;
        else if (lifecycle==Modified) return lifecycle;
        else if (lifecycle== Removed) throw new IllegalStateException("No event can occur on deleted vertices: " + event);
        else if (event==Event.DELETED_RELATION) {
            if (lifecycle==Loaded) return DeletedRelations;
            else if (lifecycle==AddedRelations) return Modified;
            else if (lifecycle==DeletedRelations) return DeletedRelations;
            else throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
        } else if (event==Event.ADDED_RELATION) {
            if (lifecycle==Loaded) return AddedRelations;
            else if (lifecycle==DeletedRelations) return Modified;
            else if (lifecycle==AddedRelations) return AddedRelations;
            else throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
        } else throw new IllegalStateException("Unexpected state: " + lifecycle + " - " + event);
    }


}

package com.thinkaurelius.titan.diskstorage.keycolumnvalue.keyvalue;

import com.thinkaurelius.titan.diskstorage.StorageException;

/**
 * A {@link KeyValueStoreManager} where the stores maintain keys in their natural order.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface OrderedKeyValueStoreManager extends KeyValueStoreManager {

    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     * <p/>
     *
     * @param name Name of database
     * @return Database Handle
     * @throws com.thinkaurelius.titan.diskstorage.StorageException
     *
     */
    @Override
    public OrderedKeyValueStore openDatabase(String name) throws StorageException;

}

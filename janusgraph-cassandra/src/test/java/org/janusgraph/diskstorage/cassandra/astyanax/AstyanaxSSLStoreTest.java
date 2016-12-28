package org.janusgraph.diskstorage.cassandra.astyanax;

import org.janusgraph.CassandraStorageSetup;
import org.janusgraph.diskstorage.configuration.ModifiableConfiguration;
import org.janusgraph.testcategory.CassandraSSLTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category({ CassandraSSLTests.class })
public class AstyanaxSSLStoreTest extends AstyanaxStoreTest {

    @BeforeClass
    public static void startCassandra() {
        CassandraStorageSetup.startCleanEmbedded();
    }

    @Override
    public ModifiableConfiguration getBaseStorageConfiguration() {
        return CassandraStorageSetup.getAstyanaxSSLConfiguration(getClass().getSimpleName());
    }
}

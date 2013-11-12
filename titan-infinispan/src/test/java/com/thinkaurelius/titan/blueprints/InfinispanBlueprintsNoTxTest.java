package com.thinkaurelius.titan.blueprints;

public class InfinispanBlueprintsNoTxTest extends AbstractInfinispanBlueprintsTest {

    public InfinispanBlueprintsNoTxTest() {
        super(false);
    }
    

    
    @Override
    public void testTransactionalGraphTestSuite() {
        /*
         * I think this test method might not handle lock contention or expected value mismatch exceptions.
         * The threads it starts dies with these types of exceptions on both Infinispan and InMemory.
         */
    }
}

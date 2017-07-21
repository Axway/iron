package io.axway.iron.core.store.relation;

import io.axway.iron.core.store.AbstractStoreTests;

public class RelationTests extends AbstractStoreTests {

    public RelationTests() {
        super( //
               new ShouldDeleteRelationTailTest(), //
               new ShouldUpdateSimpleRelationHeadTest(), //
               new ShouldRollbackRelationTest() //
        );
    }
}

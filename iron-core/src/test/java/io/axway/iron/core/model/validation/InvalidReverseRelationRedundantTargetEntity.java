package io.axway.iron.core.model.validation;

import java.util.*;
import io.axway.iron.description.DSL;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidReverseRelationRedundantTargetEntity {
    default Collection<InvalidReverseRelationRedundantEntity> reverse1() {
        return DSL.reciprocalManyRelation(InvalidReverseRelationRedundantEntity.class, InvalidReverseRelationRedundantEntity::relation);
    }

    default Collection<InvalidReverseRelationRedundantEntity> reverse2() {
        return DSL.reciprocalManyRelation(InvalidReverseRelationRedundantEntity.class, InvalidReverseRelationRedundantEntity::relation);
    }
}

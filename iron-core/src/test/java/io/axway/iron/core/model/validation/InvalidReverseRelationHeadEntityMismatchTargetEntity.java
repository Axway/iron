package io.axway.iron.core.model.validation;

import java.util.*;
import io.axway.iron.description.DSL;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidReverseRelationHeadEntityMismatchTargetEntity {
    default Collection<InvalidReverseRelationHeadEntityMismatchEntity> reverse() {
        return DSL.reciprocalManyRelation(InvalidReverseRelationHeadEntityMismatchEntity.class, InvalidReverseRelationHeadEntityMismatchEntity::relation);
    }
}

package io.axway.iron.core.model.validation;

import java.util.*;
import io.axway.iron.description.DSL;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidReverseRelationTailEntityMismatchTargetEntity {
    default Collection<InvalidReverseRelationTailEntityMismatchEntity> reverse() {
        DSL.reciprocalManyRelation(InvalidReverseRelationMissingDSLCallEntity.class, InvalidReverseRelationMissingDSLCallEntity::relation);
        return null;
    }
}

package io.axway.iron.core.model.validation;

import java.util.*;
import io.axway.iron.description.DSL;
import io.axway.iron.description.Entity;

@Entity
public interface TargetWithReverseEntity {

    default Collection<ValidEntity> reverse1() {
        return DSL.reciprocalManyRelation(ValidEntity.class, ValidEntity::relation1);
    }

    default Collection<ValidEntity> reverse2() {
        return DSL.reciprocalManyRelation(ValidEntity.class, ValidEntity::relation2);
    }

    default Collection<ValidEntity> reverse3() {
        return DSL.reciprocalManyRelation(ValidEntity.class, ValidEntity::relations);
    }
}

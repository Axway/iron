package io.axway.iron.core.model.validation;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.description.DSL;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidReverseRelationNullableTargetEntity {

    @Nullable
    default Collection<InvalidReverseRelationNullableEntity> reverse() {
        return DSL.reciprocalManyRelation(InvalidReverseRelationNullableEntity.class, InvalidReverseRelationNullableEntity::relation);
    }
}


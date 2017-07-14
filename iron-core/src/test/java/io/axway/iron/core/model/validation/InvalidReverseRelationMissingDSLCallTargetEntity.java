package io.axway.iron.core.model.validation;

import java.util.*;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidReverseRelationMissingDSLCallTargetEntity {
    default Collection<InvalidReverseRelationMissingDSLCallEntity> reverse() {
        return null;
    }
}

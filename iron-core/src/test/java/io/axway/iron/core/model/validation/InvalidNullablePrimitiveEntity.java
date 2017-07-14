package io.axway.iron.core.model.validation;

import javax.annotation.*;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidNullablePrimitiveEntity {
    @Nullable
    int value();
}

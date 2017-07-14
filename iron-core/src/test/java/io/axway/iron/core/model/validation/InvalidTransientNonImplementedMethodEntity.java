package io.axway.iron.core.model.validation;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Transient;

@Entity
public interface InvalidTransientNonImplementedMethodEntity {

    String value();

    @Transient
    String value2();
}

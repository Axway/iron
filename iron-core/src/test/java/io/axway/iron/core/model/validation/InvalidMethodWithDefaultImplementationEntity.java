package io.axway.iron.core.model.validation;

import io.axway.iron.description.Entity;

@Entity
public interface InvalidMethodWithDefaultImplementationEntity {
    default String value() {
        return "value";
    }
}

package io.axway.iron.core.model.validation;

import java.lang.reflect.Method;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidDatatypeEntity {
    Method value();
}

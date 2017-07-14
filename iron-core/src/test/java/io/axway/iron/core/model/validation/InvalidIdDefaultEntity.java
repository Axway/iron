package io.axway.iron.core.model.validation;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;

@Entity
public interface InvalidIdDefaultEntity {
    @Id
    default long id() {
        return 0;
    }
}

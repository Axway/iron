package io.axway.iron.core.model.validation;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.description.Entity;

@Entity
public interface InvalidNullableMultipleRelationEntity {

    @Nullable
    Collection<TargetEntity> relations();
}

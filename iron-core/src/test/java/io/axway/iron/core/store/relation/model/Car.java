package io.axway.iron.core.store.relation.model;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Unique;

@Entity
public interface Car {
    @Unique
    String plateNumber();

    Person owner();

    @Nullable
    Person previousOwner();

    Collection<Person> authorizedDrivers();
}

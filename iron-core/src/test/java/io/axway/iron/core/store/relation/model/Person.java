package io.axway.iron.core.store.relation.model;

import java.util.*;
import io.axway.iron.description.DSL;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Unique;

@Entity
public interface Person {
    @Unique
    String name();

    default Collection<Car> ownedCars() {
        return DSL.reciprocalManyRelation(Car.class, Car::owner);
    }

    default Collection<Car> authorizedCars() {
        return DSL.reciprocalManyRelation(Car.class, Car::authorizedDrivers);
    }
}

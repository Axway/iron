package io.axway.iron.sample.model;

import java.util.*;
import javax.annotation.*;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;
import io.axway.iron.description.Unique;

import static io.axway.iron.description.DSL.reciprocalManyRelation;

@Entity
public interface Company {
    @Id
    long id();

    @Unique
    String name();

    @Nullable
    String address();

    @Nullable
    String country();

    default Collection<Person> employees() {
        return reciprocalManyRelation(Person.class, Person::worksAt);
    }

    default Collection<Person> previousEmployees() {
        return reciprocalManyRelation(Person.class, Person::previousCompanies);
    }
}

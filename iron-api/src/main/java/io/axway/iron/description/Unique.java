package io.axway.iron.description;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import javax.annotation.*;
import io.axway.iron.error.UniqueConstraintViolationException;

/**
 * This annotation can be used on entity members to declare a unique constraint. So two distinct entity instances cannot have the same value for the member
 * declared as a unique constraint.<br>
 * For {@link Nullable} members, the {@code null} value is not considered for the unique constraint, so many entity instance can have a {@code null} value for this member.<br>
 * Unique constraints are enforced at modification time, in case of unique constraint violation a {@link UniqueConstraintViolationException} is thrown.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Unique {
}

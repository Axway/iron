package io.axway.iron.core.internal.definition;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import javax.annotation.*;
import com.google.common.collect.ImmutableSet;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.description.Transient;
import io.axway.iron.error.InvalidModelException;

import static io.axway.iron.description.Constants.RESERVED_PROPERTY_PREFIX;

public class InterfaceValidator {
    private static final Set<String> FORBIDDEN_METHOD_NAMES;

    static {
        ImmutableSet.Builder<String> forbiddenMethodNames = ImmutableSet.builder();
        for (Method method : Object.class.getDeclaredMethods()) {
            if (method.getParameterCount() == 0) {
                forbiddenMethodNames.add(method.getName());
            }
        }
        FORBIDDEN_METHOD_NAMES = forbiddenMethodNames.build();
    }

    private final IntrospectionHelper m_introspectionHelper;
    private final DataTypeManager m_dataTypeManager;

    public InterfaceValidator(IntrospectionHelper introspectionHelper, DataTypeManager dataTypeManager) {
        m_introspectionHelper = introspectionHelper;
        m_dataTypeManager = dataTypeManager;
    }

    public <T> void validate(String errorLabel, Class<T> clazz, InterfaceVisitor visitor) {
        String argPrefix = errorLabel.toLowerCase();
        if (!clazz.isInterface()) {
            throw new InvalidModelException(errorLabel + " class is not an interface", args -> args.add(argPrefix + "ClassName", clazz.getName()));
        }

        if ((clazz.getModifiers() & Modifier.PUBLIC) == 0) {
            throw new InvalidModelException(errorLabel + " interface is not public", args -> args.add(argPrefix + "ClassName", clazz.getName()));
        }

        visitor.visitInterface(clazz);

        if (clazz.getDeclaredMethods().length == 0) {
            throw new InvalidModelException(errorLabel + " interface doesn't declare any methods", args -> args.add(argPrefix + "ClassName", clazz.getName()));
        }

        for (Method method : clazz.getDeclaredMethods()) {

            String methodName = method.getName();
            if (methodName.startsWith(RESERVED_PROPERTY_PREFIX) || FORBIDDEN_METHOD_NAMES.contains(methodName)) {
                throw new InvalidModelException(errorLabel + " interface contains a forbidden method name",
                                                args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName));
            }

            // ignore static methods
            if ((method.getModifiers() & Modifier.STATIC) != 0) {
                continue;
            }

            if (!visitor.shouldVisitMethod(method)) {
                continue; // handle special methods like Command#execute
            }

            Annotation transientAnnotation = method.getAnnotation(Transient.class);
            if (transientAnnotation != null) {
                if (method.isDefault()) {
                    // ignore the user implemented method
                    continue;
                } else {
                    throw new InvalidModelException(errorLabel + " method is @Transient but has no default implementation",
                                                    args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName));
                }
            }

            if (method.getParameterCount() > 0) {
                throw new InvalidModelException(errorLabel + " method has parameters, only no parameters methods are supported",
                                                args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName));
            }
            if (method.getExceptionTypes().length > 0) {
                throw new InvalidModelException(errorLabel + " method must not declare exceptions",
                                                args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName));
            }

            Class<?> returnType = method.getReturnType();
            if (Void.TYPE.equals(returnType)) {
                throw new InvalidModelException(errorLabel + " method is a void method, only non void methods are supported",
                                                args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName));
            }

            Annotation nullableAnnotation = method.getAnnotation(Nullable.class);
            Annotation nonnullAnnotation = method.getAnnotation(Nonnull.class);
            if (nullableAnnotation != null && nonnullAnnotation != null) {
                throw new InvalidModelException(errorLabel + " method cannot be both @Nonnull and @Nullable",
                                                args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName));
            }

            Class<?> collectionElementType = null;
            if (Collection.class.isAssignableFrom(returnType)) {
                if (nullableAnnotation != null) {
                    throw new InvalidModelException(errorLabel + " relation is a multiple relation that cannot be @Nullable",
                                                    args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName));
                }

                collectionElementType = m_introspectionHelper.getParametrizedClass(method.getGenericReturnType(), 0);
            } else {
                boolean nullable = nullableAnnotation != null;
                if (nullable) {
                    if (returnType.isPrimitive()) {
                        throw new InvalidModelException(errorLabel + " attribute is @Nullable but has a primitive datatype that cannot be null",
                                                        args -> args.add(argPrefix + "ClassName", clazz.getName()).add("methodName", methodName)
                                                                .add("dataType", returnType.getName()));
                    }
                } else {
                    Class<?> primitiveClass = m_dataTypeManager.getPrimitiveTypeOf(returnType);
                    if (primitiveClass != null) {
                        throw new InvalidModelException(
                                errorLabel + " attribute is not @Nullable but has a primitive wrapper datatype, primitive datatype must be used instead",
                                args -> args.add(argPrefix + "ClassName", clazz.getName()) //
                                        .add("methodName", methodName).add("dataType", returnType.getName()) //
                                        .add("primitiveDataType", primitiveClass));
                    }
                }
            }

            if (collectionElementType != null) {
                visitor.visitMethod(method, collectionElementType, true, false);
            } else {
                visitor.visitMethod(method, returnType, false, nullableAnnotation != null);
            }
        }
    }
}

package io.axway.iron.core.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.*;
import java.util.regex.*;
import javax.annotation.*;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import io.axway.iron.core.StoreManagerFactoryBuilder;
import io.axway.iron.error.ConfigurationException;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;

import static java.util.Arrays.*;

public class StoreManagerFactoryBuilderConfigurator {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerFactoryBuilderConfigurator.class);
    private static final String ENV_PREFIX = "env:".toLowerCase();
    private static final Pattern VARIABLE_REGEX_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");
    private static final Object UNKNOWN_KEY = new Object();

    public void fill(StoreManagerFactoryBuilder storeManagerFactoryBuilder, Properties properties) {
        Map<Type, BuilderImplConfig> componentBuilders = findComponentBuilders(properties);
        for (Map.Entry<Type, BuilderImplConfig> entry : componentBuilders.entrySet()) {
            BuilderImplConfig config = entry.getValue();
            Supplier<Object> builder = instantiateBuilder(config.supplierClass(), properties, config.baseName());
            buildAndAssign(builder, entry.getKey(), storeManagerFactoryBuilder);
        }
    }

    private Map<Type, BuilderImplConfig> findComponentBuilders(Properties properties) {
        Map<Type, BuilderImplConfig> map = new HashMap<>();  // ComponentBuilder -> property name base
        for (String propertyName : properties.stringPropertyNames()) {
            String className = getProperty(properties, propertyName);
            if (className != null) {
                try {
                    Class<?> clazz = Class.forName(className);
                    if (Supplier.class.isAssignableFrom(clazz)) {
                        @SuppressWarnings("unchecked") Class<Supplier<Object>> supplierClass = (Class<Supplier<Object>>) clazz;
                        Type type = ((ParameterizedType) clazz.getGenericInterfaces()[0]).getActualTypeArguments()[0];
                        if (map.containsKey(type)) {
                            LOG.warn("Supplier is already defined",
                                     args -> args.add("supplierType", type.getTypeName()).add("existingSupplier", map.get(type).supplierClass().getName()));
                        }
                        BuilderImplConfig builderImplConfig = new BuilderImplConfig(supplierClass, propertyName);
                        LOG.info("Adding a supplier", args -> args.add("supplierType", type.getTypeName()).add("addedSupplier", className));
                        map.put(type, builderImplConfig);
                    }
                } catch (ClassNotFoundException e) {
                    // nothing to do
                }
            }
        }
        return map;
    }

    private <T> Supplier<T> instantiateBuilder(Class<Supplier<T>> componentBuilderClazz, Properties properties, String baseName) {
        try {
            Constructor<Supplier<T>> constructor = componentBuilderClazz.getConstructor();
            Supplier<T> builder = constructor.newInstance();

            for (Method method : componentBuilderClazz.getDeclaredMethods()) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                if (method.getName().startsWith("set") && (parameterTypes.length == 1)) {
                    String propertyName = method.getName().substring(3);
                    Object value = getPropertyValue(parameterTypes[0], properties, baseName, propertyName);
                    if (isMandatory(method) && (value == null || UNKNOWN_KEY.equals(value))) {
                        throw new ConfigurationException("A mandatory parameter is missing for builder", //
                                                         args -> args.add("builder", componentBuilderClazz.getName()).add("propertyName", propertyName));
                    } else if (!UNKNOWN_KEY.equals(value)) { // do not call setter if the key was absent from properties
                        method.invoke(builder, value);
                    }
                }
            }
            return builder;
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException(e);
        }
    }

    private boolean isMandatory(Method method) {
        return stream(method.getParameterAnnotations()[0]).noneMatch(x -> Nullable.class.isAssignableFrom(x.annotationType()));
    }

    @Nullable
    private <T> Object getPropertyValue(Class<T> clazz, Properties properties, String baseName, String propertyName) {
        if (clazz.isAssignableFrom(Properties.class)) {
            return extractProperties(properties, baseName);
        } else {
            String fullName = baseName + "." + camelCaseToLowerCaseUnderscore(propertyName);
            if (!properties.containsKey(fullName)) {
                return UNKNOWN_KEY;
            } else {
                String propertyValue = getProperty(properties, fullName);
                if (propertyValue == null || clazz.isAssignableFrom(String.class)) {
                    return propertyValue;
                } else if (clazz.isAssignableFrom(Path.class)) {
                    return extractPath(propertyValue);
                } else {
                    return extractStandardTypes(clazz, propertyValue);
                }
            }
        }
    }

    private String camelCaseToLowerCaseUnderscore(String propertyName) {
        return propertyName.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
    }

    @Nullable
    private Path extractPath(String value) {
        try {
            return value.isEmpty() ? null : Paths.get(value);
        } catch (InvalidPathException e) {
            throw new ConfigurationException(e);
        }
    }

    private Properties extractProperties(Properties properties, String baseName) {
        Properties value = new Properties();
        int basenameLength = baseName.length() + 1;
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith(baseName) && !name.equals(baseName)) { // startWith but not the same
                value.put(name.substring(basenameLength), getProperty(properties, name)); // There can't be null value here
            }
        }
        return value;
    }

    @Nullable
    private <T> Object extractStandardTypes(Class<T> clazz, String value) {
        try {
            return value.isEmpty() ? null : clazz.getDeclaredMethod("valueOf", String.class).invoke(null, value);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new ConfigurationException(e);
        }
    }

    private String getProperty(Properties properties, String key) {
        String property = properties.getProperty(key);
        if (property != null) {
            if (property.toLowerCase().startsWith(ENV_PREFIX)) {
                // If the value retrieved start with the environment prefix, we retrieve the key value from the System environment
                // e.g: key=ENV:AWS_ACCESS_KEY_ID -> System.getenv("AWS_ACCESS_KEY_ID")
                String envVarName = property.substring(ENV_PREFIX.length());
                property = System.getenv(envVarName);
            } else {
                // If the value retrieved follow the variable pattern, we extract the value, and use it as a key to retrieve the real value.
                // e.g: key=${io.iron.configuration.value}  -> properties.getProperty("io.iron.configuration.value")
                Matcher matcher = VARIABLE_REGEX_PATTERN.matcher(property);
                if (matcher.find()) {
                    property = getProperty(properties, matcher.group(1));  // beware recursive call
                }
            }
        }
        return property;
    }

    private void buildAndAssign(Supplier<Object> builder, Type type, StoreManagerFactoryBuilder storeManagerFactoryBuilder) {
        Object component = builder.get();
        Class<?> typeClass = (Class<?>) type;
        if (typeClass.isAssignableFrom(TransactionSerializer.class)) {
            storeManagerFactoryBuilder.withTransactionSerializer((TransactionSerializer) component);
        } else if (typeClass.isAssignableFrom(TransactionStoreFactory.class)) {
            storeManagerFactoryBuilder.withTransactionStoreFactory((TransactionStoreFactory) component);
        } else if (typeClass.isAssignableFrom(SnapshotSerializer.class)) {
            storeManagerFactoryBuilder.withSnapshotSerializer((SnapshotSerializer) component);
        } else if (typeClass.isAssignableFrom(SnapshotStoreFactory.class)) {
            storeManagerFactoryBuilder.withSnapshotStoreFactory((SnapshotStoreFactory) component);
        } else {
            LOG.error("Supplier type is not supported", args -> args.add("supplierType", typeClass).add("supplier", component.getClass().getName()));
        }
    }

    private static class BuilderImplConfig {
        private Class<Supplier<Object>> m_supplierClass;
        private String m_baseName;

        BuilderImplConfig(Class<Supplier<Object>> supplierClass, String baseName) {
            m_supplierClass = supplierClass;
            m_baseName = baseName;
        }

        Class<Supplier<Object>> supplierClass() {
            return m_supplierClass;
        }

        String baseName() {
            return m_baseName;
        }
    }
}

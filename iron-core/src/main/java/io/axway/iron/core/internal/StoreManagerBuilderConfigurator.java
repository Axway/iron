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
import com.google.common.annotations.VisibleForTesting;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;
import io.axway.iron.core.StoreManagerBuilder;
import io.axway.iron.error.ConfigurationException;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStore;
import io.axway.iron.spi.storage.TransactionStore;

import static java.util.Arrays.*;

public class StoreManagerBuilderConfigurator {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerBuilderConfigurator.class);
    private static final Pattern VARIABLE_REGEX_PATTERN = Pattern.compile("\\$\\{(?:(?<source>env|ENV|sys|SYS):)?(?<name>[^}].*?)}");
    private static final Object UNKNOWN_KEY = new Object();

    public void fill(StoreManagerBuilder storeManagerBuilder, String builderName, Properties properties) {
        Map<Type, BuilderImplConfig> componentBuilders = findComponentBuilders(properties);
        for (Map.Entry<Type, BuilderImplConfig> entry : componentBuilders.entrySet()) {
            BuilderImplConfig config = entry.getValue();
            Supplier<Object> builder = instantiateBuilder(config.supplierClass(), properties, builderName, config.baseName());
            buildAndAssign(builder, entry.getKey(), storeManagerBuilder);
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

    private <T> Supplier<T> instantiateBuilder(Class<Supplier<T>> componentBuilderClazz, Properties properties, String builderName, String baseName) {
        try {
            Supplier<T> builder;
            try {
                Constructor<Supplier<T>> constructor = componentBuilderClazz.getConstructor(String.class);
                builder = constructor.newInstance(builderName);
            } catch (NoSuchMethodException e) {
                Constructor<Supplier<T>> constructor = componentBuilderClazz.getConstructor();
                builder = constructor.newInstance();
            }

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

    @VisibleForTesting
    static String getProperty(Properties properties, String key) {
        String property = properties.getProperty(key);
        if (property != null) {
            StringBuffer result = new StringBuffer();
            Matcher matcher = VARIABLE_REGEX_PATTERN.matcher(property);
            while (matcher.find()) {
                String name = matcher.group("name");
                String source = matcher.group("source");
                String value = null;
                if (source == null) {
                    // If the value retrieved follow the variable pattern, we extract the value, and use it as a key to retrieve the real value.
                    // e.g: key=${io.iron.configuration.value}  -> properties.getProperty("io.iron.configuration.value")
                    value = getProperty(properties, name);  // beware recursive call
                } else if (source.equalsIgnoreCase("env")) {
                    // If the value retrieved start with the environment prefix, we retrieve the key value from the System environment
                    // e.g: key=ENV:AWS_ACCESS_KEY_ID -> System.getenv("AWS_ACCESS_KEY_ID")
                    value = System.getenv(name);
                } else if (source.equalsIgnoreCase("sys")) {
                    // If the value retrieved start with the environment prefix, we retrieve the key value from the System environment
                    // e.g: key=SYS:io.axway.aws_access_key_id -> System.getProperty("AWS_ACCESS_KEY_ID")
                    value = System.getProperty(name);
                }
                if (value == null) {
                    throw new ConfigurationException("Unknown property", args -> args.add("key", key).add("properties", properties));
                }
                matcher.appendReplacement(result, Matcher.quoteReplacement(value));
            }
            matcher.appendTail(result);
            property = result.toString();
        }
        return property;
    }

    private void buildAndAssign(Supplier<Object> builder, Type type, StoreManagerBuilder storeManagerBuilder) {
        Object component = builder.get();
        Class<?> typeClass = (Class<?>) type;
        if (typeClass.isAssignableFrom(TransactionSerializer.class)) {
            storeManagerBuilder.withTransactionSerializer((TransactionSerializer) component);
        } else if (typeClass.isAssignableFrom(TransactionStore.class)) {
            storeManagerBuilder.withTransactionStore((TransactionStore) component);
        } else if (typeClass.isAssignableFrom(SnapshotSerializer.class)) {
            storeManagerBuilder.withSnapshotSerializer((SnapshotSerializer) component);
        } else if (typeClass.isAssignableFrom(SnapshotStore.class)) {
            storeManagerBuilder.withSnapshotStore((SnapshotStore) component);
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

        private String baseName() {
            return m_baseName;
        }
    }
}

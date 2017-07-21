package io.axway.iron.core.internal.definition;

import java.util.*;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.axway.iron.core.internal.utils.TypeConverter;
import io.axway.iron.error.StoreException;

public class DataTypeManager {
    private static final Set<Class<?>> JDK_DATA_TYPES = ImmutableSet.of(boolean.class, Boolean.class, //
                                                                        byte.class, Byte.class, //
                                                                        char.class, Character.class, //
                                                                        short.class, Short.class, //
                                                                        int.class, Integer.class, //
                                                                        long.class, Long.class, //
                                                                        float.class, Float.class, //
                                                                        double.class, Double.class, //
                                                                        String.class, //
                                                                        Date.class);

    private static final Map<Class<?>, Class<?>> PRIMITIVES_TYPES_BY_WRAPPER = ImmutableMap.<Class<?>, Class<?>>builder() //
            .put(Boolean.class, boolean.class) //
            .put(Byte.class, byte.class) //
            .put(Character.class, char.class) //
            .put(Short.class, short.class) //
            .put(Integer.class, int.class) //
            .put(Long.class, long.class) //
            .put(Float.class, float.class) //
            .put(Double.class, double.class) //
            .build();

    private static final Map<Class<?>, TypeConverter<?>> TYPE_CONVERTERS;

    static {
        Map<Class<?>, TypeConverter<?>> converters = new HashMap<>();
        converters.put(Byte.class, value -> {
            if (value == null || value instanceof Byte) {
                return value;
            } else {
                return ((Number) value).byteValue();
            }
        });
        converters.put(byte.class, converters.get(Byte.class));

        converters.put(Character.class, value -> {
            if (value == null || value instanceof Character) {
                return value;
            } else if (value instanceof String) {
                String s = (String) value;
                if (s.length() == 1) {
                    return s.charAt(0);
                }
            }
            throw new StoreException("Unable to convert type", arguments -> //
                    arguments.add("source", value.getClass().getName()) //
                            .add("target", Character.class.getName()) //
                            .add("value", value));
        });
        converters.put(char.class, converters.get(Character.class));

        converters.put(Short.class, value -> {
            if (value == null || value instanceof Short) {
                return value;
            } else {
                return ((Number) value).shortValue();
            }
        });
        converters.put(short.class, converters.get(Short.class));

        converters.put(Integer.class, value -> {
            if (value == null || value instanceof Integer) {
                return value;
            } else {
                return ((Number) value).intValue();
            }
        });
        converters.put(int.class, converters.get(Integer.class));

        converters.put(Long.class, value -> {
            if (value == null || value instanceof Long) {
                return value;
            } else {
                return ((Number) value).longValue();
            }
        });
        converters.put(long.class, converters.get(Long.class));

        converters.put(Float.class, value -> {
            if (value == null || value instanceof Float) {
                return value;
            } else {
                return ((Number) value).floatValue();
            }
        });
        converters.put(float.class, converters.get(Float.class));

        converters.put(Double.class, value -> {
            if (value == null || value instanceof Double) {
                return value;
            } else {
                return ((Number) value).doubleValue();
            }
        });
        converters.put(double.class, converters.get(Double.class));

        converters.put(Date.class, value -> {
            if (value == null || value instanceof Date) {
                return value;
            } else if (value instanceof Long) {
                return new Date((long) value);
            }
            throw new StoreException("Unable to convert type", arguments -> //
                    arguments.add("source", value.getClass().getName()) //
                            .add("target", Date.class.getName()) //
                            .add("value", value));
        });

        TYPE_CONVERTERS = ImmutableMap.copyOf(converters);
    }

    public boolean isValidDataType(Class<?> dataTypeClass) {
        return JDK_DATA_TYPES.contains(dataTypeClass);
    }

    public Class<?> getPrimitiveTypeOf(Class<?> clazz) {
        return PRIMITIVES_TYPES_BY_WRAPPER.get(clazz);
    }

    //TODO very ugly fix
    @SuppressWarnings("unchecked")
    public <T> TypeConverter<T> getTypeConverter(Class<T> clazz) {
        TypeConverter<T> converter = (TypeConverter<T>) TYPE_CONVERTERS.get(clazz);
        if (converter == null) {
            converter = value -> (T) value;
        }

        return converter;
    }

    public <T> TypeConverter<Collection<T>> getCollectionTypeConverter(Class<T> clazz) {
        TypeConverter<T> elementConverter = getTypeConverter(clazz);
        return value -> {
            if (value == null) {
                return null;
            }

            Collection<?> params = Collection.class.cast(value);
            Collection<T> converted = new ArrayList<>(params.size());
            for (Object param : params) {
                converted.add(elementConverter.convert(param));
            }

            return converted;
        };
    }
}

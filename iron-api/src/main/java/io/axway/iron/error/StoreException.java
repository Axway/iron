package io.axway.iron.error;

import java.io.*;
import java.util.*;
import java.util.function.*;

/**
 * The root exception for all exception related to store.
 */
public class StoreException extends RuntimeException {
    @FunctionalInterface
    public interface Arguments extends Serializable {
        Arguments add(String key, Object value);
    }

    private final Consumer<Arguments> m_argsConsumer;

    public StoreException() {
        m_argsConsumer = null;
    }

    public StoreException(String message) {
        super(message);
        m_argsConsumer = null;
    }

    public StoreException(Throwable cause) {
        super(cause);
        m_argsConsumer = null;
    }

    public StoreException(Throwable cause, String message) {
        super(message, cause);
        m_argsConsumer = null;
    }

    public StoreException(String message, Consumer<Arguments> args) {
        super(message);
        m_argsConsumer = args;
    }

    public StoreException(String message, Consumer<Arguments> args, Throwable cause) {
        super(message, cause);
        m_argsConsumer = args;
    }

    @Override
    public String getMessage() {
        if (m_argsConsumer != null) {
            StringBuilder sb = new StringBuilder();
            sb.append(super.getMessage()).append(" {");
            StringJoiner joiner = new StringJoiner(", ");
            m_argsConsumer.accept(new Arguments() {
                @Override
                public Arguments add(String key, Object value) {
                    joiner.add(key + ": " + format(value));
                    return this;
                }
            });
            sb.append(joiner);
            sb.append("}");
            return sb.toString();
        } else {
            return super.getMessage();
        }
    }

    private static String format(Object object) {
        if (object instanceof String) {
            return quote((String) object);
        } else if (object instanceof Collection) {
            Collection<?> c = (Collection<?>) object;
            StringJoiner stringJoiner = new StringJoiner(", ");
            for (Object o : c) {
                stringJoiner.add(format(o));
            }
            return '[' + stringJoiner.toString() + ']';
        } else {
            return String.valueOf(object);
        }
    }

    private static String quote(String string) {
        int len = string.length();
        StringBuilder sb = new StringBuilder(len + 2);
        sb.append('"');
        for (int i = 0; i < len; i++) {
            char c = string.charAt(i);
            if (c < ' ') {
                switch (c) {
                    case '\b':
                        sb.append("\\b");
                        break;
                    case '\t':
                        sb.append("\\t");
                        break;
                    case '\n':
                        sb.append("\\n");
                        break;
                    case '\f':
                        sb.append("\\f");
                        break;
                    case '\r':
                        sb.append("\\r");
                        break;
                    default:
                        String codePoint = "000" + Integer.toHexString(c);
                        sb.append("\\u").append(codePoint, codePoint.length() - 4, 4);
                        break;
                }
            } else if (c == '"' || c == '\\' || c == '/') {
                sb.append('\\').append(c);
            } else {
                sb.append(c);
            }
        }
        sb.append('"');
        return sb.toString();
    }
}

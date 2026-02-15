package io.memris.runtime;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

final class PropertyPathAccessor {

    private final String path;
    private final Field[] fields;
    private final Constructor<?>[] intermediateConstructors;

    private PropertyPathAccessor(String path, Field[] fields, Constructor<?>[] intermediateConstructors) {
        this.path = path;
        this.fields = fields;
        this.intermediateConstructors = intermediateConstructors;
    }

    static PropertyPathAccessor compile(Class<?> rootType, String path) {
        if (rootType == null) {
            throw new IllegalArgumentException("rootType required");
        }
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("path required");
        }

        String[] segments = path.split("\\.");
        Field[] chain = new Field[segments.length];
        Constructor<?>[] ctors = new Constructor<?>[Math.max(segments.length - 1, 0)];
        Class<?> current = rootType;

        for (int i = 0; i < segments.length; i++) {
            Field field = findField(current, segments[i]);
            if (field == null) {
                throw new IllegalArgumentException("Field path not found: " + rootType.getName() + "#" + path);
            }
            field.setAccessible(true);
            chain[i] = field;

            if (i < segments.length - 1) {
                Class<?> nextType = field.getType();
                Constructor<?> ctor = resolveNoArgConstructor(nextType, rootType, path);
                ctors[i] = ctor;
                current = nextType;
            }
        }

        return new PropertyPathAccessor(path, chain, ctors);
    }

    Object get(Object root) {
        Object current = root;
        for (Field field : fields) {
            if (current == null) {
                return null;
            }
            try {
                current = field.get(current);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to read path: " + path, e);
            }
        }
        return current;
    }

    void set(Object root, Object value) {
        if (root == null) {
            return;
        }

        Object current = root;
        for (int i = 0; i < fields.length - 1; i++) {
            Field field = fields[i];
            Object next;
            try {
                next = field.get(current);
                if (next == null) {
                    next = intermediateConstructors[i].newInstance();
                    field.set(current, next);
                }
            } catch (ReflectiveOperationException e) {
                throw new RuntimeException("Failed to create nested value for path: " + path, e);
            }
            current = next;
        }

        Field leaf = fields[fields.length - 1];
        if (value == null && leaf.getType().isPrimitive()) {
            return;
        }

        try {
            leaf.set(current, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to write path: " + path, e);
        }
    }

    private static Field findField(Class<?> type, String name) {
        Class<?> current = type;
        while (current != null && current != Object.class) {
            try {
                return current.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                current = current.getSuperclass();
            }
        }
        return null;
    }

    private static Constructor<?> resolveNoArgConstructor(Class<?> type, Class<?> rootType, String path) {
        try {
            Constructor<?> ctor = type.getDeclaredConstructor();
            if (!Modifier.isPublic(ctor.getModifiers())) {
                ctor.setAccessible(true);
            }
            return ctor;
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException(
                    "Embedded path requires no-arg constructor: " + rootType.getName() + "#" + path, e);
        }
    }
}

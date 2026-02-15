package io.memris.core;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Field;

/**
 * Immutable, pre-compiled access plan for a persisted property path.
 * <p>
 * Plans are compiled once at metadata extraction time and reused by
 * generated/runtime accessors across save/materialize/update/index paths.
 */
public final class ColumnAccessPlan {

    private final Class<?> rootType;
    private final String propertyPath;
    private final int columnIndex;
    private final byte typeCode;
    private final boolean relationship;
    private final boolean collection;
    private final boolean embedded;
    private final boolean primitiveLeaf;
    private final MethodHandle[] getters;
    private final MethodHandle[] setters;
    private final MethodHandle[] intermediateConstructors;

    private ColumnAccessPlan(
            Class<?> rootType,
            String propertyPath,
            int columnIndex,
            byte typeCode,
            boolean relationship,
            boolean collection,
            boolean embedded,
            boolean primitiveLeaf,
            MethodHandle[] getters,
            MethodHandle[] setters,
            MethodHandle[] intermediateConstructors) {
        this.rootType = rootType;
        this.propertyPath = propertyPath;
        this.columnIndex = columnIndex;
        this.typeCode = typeCode;
        this.relationship = relationship;
        this.collection = collection;
        this.embedded = embedded;
        this.primitiveLeaf = primitiveLeaf;
        this.getters = getters;
        this.setters = setters;
        this.intermediateConstructors = intermediateConstructors;
    }

    public static ColumnAccessPlan compile(Class<?> rootType, String propertyPath) {
        return compile(rootType, propertyPath, -1, TypeCodes.TYPE_LONG, false, false, propertyPath.indexOf('.') >= 0);
    }

    public static ColumnAccessPlan compile(Class<?> rootType, EntityMetadata.FieldMapping mapping) {
        if (mapping == null) {
            throw new IllegalArgumentException("mapping required");
        }
        return compile(
                rootType,
                mapping.name(),
                mapping.columnPosition(),
                mapping.typeCode(),
                mapping.isRelationship(),
                mapping.isCollection(),
                mapping.isEmbedded());
    }

    public static ColumnAccessPlan compile(
            Class<?> rootType,
            String propertyPath,
            int columnIndex,
            byte typeCode,
            boolean relationship,
            boolean collection,
            boolean embedded) {
        if (rootType == null) {
            throw new IllegalArgumentException("rootType required");
        }
        if (propertyPath == null || propertyPath.isBlank()) {
            throw new IllegalArgumentException("propertyPath required");
        }

        String[] segments = propertyPath.split("\\.");
        MethodHandle[] getters = new MethodHandle[segments.length];
        MethodHandle[] setters = new MethodHandle[segments.length];
        MethodHandle[] ctors = new MethodHandle[Math.max(segments.length - 1, 0)];
        Class<?> current = rootType;
        boolean primitiveLeaf = false;

        for (int i = 0; i < segments.length; i++) {
            Field field = findField(current, segments[i]);
            if (field == null) {
                throw new IllegalArgumentException("Field path not found: " + rootType.getName() + "#" + propertyPath);
            }
            try {
                MethodHandles.Lookup privateLookup = MethodHandles.privateLookupIn(
                        field.getDeclaringClass(),
                        MethodHandles.lookup());
                getters[i] = privateLookup.unreflectGetter(field)
                        .asType(MethodType.methodType(Object.class, Object.class));
                setters[i] = privateLookup.unreflectSetter(field)
                        .asType(MethodType.methodType(void.class, Object.class, Object.class));
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException("Failed to access path: " + rootType.getName() + "#" + propertyPath, e);
            }

            if (i == segments.length - 1) {
                primitiveLeaf = field.getType().isPrimitive();
            }

            if (i < segments.length - 1) {
                Class<?> nextType = field.getType();
                ctors[i] = resolveNoArgConstructor(nextType, rootType, propertyPath);
                current = nextType;
            }
        }

        return new ColumnAccessPlan(
                rootType,
                propertyPath,
                columnIndex,
                typeCode,
                relationship,
                collection,
                embedded,
                primitiveLeaf,
                getters,
                setters,
                ctors);
    }

    public Class<?> rootType() {
        return rootType;
    }

    public String propertyPath() {
        return propertyPath;
    }

    public int columnIndex() {
        return columnIndex;
    }

    public byte typeCode() {
        return typeCode;
    }

    public boolean relationship() {
        return relationship;
    }

    public boolean collection() {
        return collection;
    }

    public boolean embedded() {
        return embedded;
    }

    public boolean primitiveLeaf() {
        return primitiveLeaf;
    }

    public int segmentCount() {
        return getters.length;
    }

    public boolean multiSegment() {
        return getters.length > 1;
    }

    public Object get(Object root) {
        Object current = root;
        for (MethodHandle getter : getters) {
            if (current == null) {
                return null;
            }
            current = invokeGetter(getter, current);
        }
        return current;
    }

    public void set(Object root, Object value) {
        if (root == null) {
            return;
        }

        Object current = root;
        for (int i = 0; i < getters.length - 1; i++) {
            Object next = invokeGetter(getters[i], current);
            if (next == null) {
                next = invokeConstructor(intermediateConstructors[i]);
                invokeSetter(setters[i], current, next);
            }
            current = next;
        }

        if (value == null && primitiveLeaf) {
            return;
        }
        invokeSetter(setters[setters.length - 1], current, value);
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

    private static MethodHandle resolveNoArgConstructor(Class<?> type, Class<?> rootType, String propertyPath) {
        try {
            MethodHandles.Lookup privateLookup = MethodHandles.privateLookupIn(type, MethodHandles.lookup());
            return privateLookup.findConstructor(type, MethodType.methodType(void.class))
                    .asType(MethodType.methodType(Object.class));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new IllegalArgumentException(
                    "Embedded path requires no-arg constructor: " + rootType.getName() + "#" + propertyPath,
                    e);
        }
    }

    private Object invokeGetter(MethodHandle getter, Object instance) {
        try {
            return getter.invoke(instance);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to read path: " + propertyPath, e);
        }
    }

    private Object invokeConstructor(MethodHandle constructor) {
        try {
            return constructor.invoke();
        } catch (Throwable e) {
            throw new RuntimeException("Failed to create nested value for path: " + propertyPath, e);
        }
    }

    private Object invokeSetter(MethodHandle setter, Object instance, Object value) {
        try {
            return setter.invoke(instance, value);
        } catch (Throwable e) {
            throw new RuntimeException("Failed to write path: " + propertyPath, e);
        }
    }
}

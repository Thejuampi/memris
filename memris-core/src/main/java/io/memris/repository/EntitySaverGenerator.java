package io.memris.repository;

import io.memris.core.ColumnAccessPlan;
import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.Id;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import io.memris.runtime.EntitySaver;
import io.memris.storage.GeneratedTable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Generates EntitySaver implementations using ByteBuddy with generated hot paths.
 * <p>
 * Flat public single-segment fields keep direct field-bytecode access.
 * Embedded/multi-segment fields use precompiled {@link ColumnAccessPlan}.
 */
public final class EntitySaverGenerator {
    private static final ConcurrentHashMap<ShapeKey, EntitySaver<?, ?>> SAVER_CACHE = new ConcurrentHashMap<>();
    private static final String RELATIONSHIP_HANDLE_MAP_FIELD = "relationshipHandleMap";

    public static <T> EntitySaver<T, ?> generate(Class<T> entityClass, EntityMetadata<T> metadata) {
        ShapeKey key = ShapeKey.from(metadata);
        @SuppressWarnings("unchecked")
        EntitySaver<T, ?> cached = (EntitySaver<T, ?>) SAVER_CACHE.get(key);
        if (cached != null) {
            return cached;
        }
        EntitySaver<T, ?> generated = generateUncached(entityClass, metadata);
        @SuppressWarnings("unchecked")
        EntitySaver<T, ?> existing = (EntitySaver<T, ?>) SAVER_CACHE.putIfAbsent(key, generated);
        return existing != null ? existing : generated;
    }

    static void clearCacheForTests() {
        SAVER_CACHE.clear();
    }

    private static <T> EntitySaver<T, ?> generateUncached(Class<T> entityClass, EntityMetadata<T> metadata) {
        var idField = resolveIdField(entityClass, metadata);
        var fields = resolveFields(entityClass, metadata, idField.field.getName());
        var converterFields = fields.stream().filter(f -> f.converter != null).toList();
        var planFields = fields.stream().filter(f -> f.plan != null).toList();
        var relationshipFields = fields.stream().filter(FieldInfo::isRelationship).toList();
        var resolverRelationshipHandles = resolveRelationshipHandles(entityClass, metadata);
        boolean hasResolverRelationships = !resolverRelationshipHandles.isEmpty();

        assignConverterFieldNames(converterFields);
        for (int i = 0; i < planFields.size(); i++) {
            planFields.get(i).planIndex = i;
        }
        for (int i = 0; i < relationshipFields.size(); i++) {
            relationshipFields.get(i).idHandleIndex = i;
        }

        int idColumnIndex = metadata.resolveColumnPosition(metadata.idColumnName());
        int columnCount = metadata.fields().stream()
                .filter(mapping -> mapping.columnPosition() >= 0)
                .mapToInt(FieldMapping::columnPosition)
                .max()
                .orElse(-1) + 1;
        if (columnCount <= 0) {
            throw new IllegalStateException("No persisted columns for " + entityClass.getName());
        }

        var implName = entityClass.getName() + "$MemrisSaver$" + System.nanoTime();
        var builder = new ByteBuddy()
                .subclass(Object.class)
                .implement(EntitySaver.class)
                .name(implName)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        for (var converterField : converterFields) {
            builder = builder.defineField(converterField.converterFieldName, TypeConverter.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }
        for (int i = 0; i < planFields.size(); i++) {
            builder = builder.defineField(planFieldName(i), ColumnAccessPlan.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }
        for (int i = 0; i < relationshipFields.size(); i++) {
            builder = builder.defineField(idHandleFieldName(i), MethodHandle.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }
        if (hasResolverRelationships) {
            builder = builder.defineField(RELATIONSHIP_HANDLE_MAP_FIELD, Map.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }

        int totalParams = converterFields.size() + planFields.size() + relationshipFields.size()
                + (hasResolverRelationships ? 1 : 0);
        if (totalParams > 0) {
            var paramTypes = new Class<?>[totalParams];
            int argIndex = 0;
            for (int i = 0; i < converterFields.size(); i++) {
                paramTypes[argIndex++] = TypeConverter.class;
            }
            for (int i = 0; i < planFields.size(); i++) {
                paramTypes[argIndex++] = ColumnAccessPlan.class;
            }
            for (int i = 0; i < relationshipFields.size(); i++) {
                paramTypes[argIndex++] = MethodHandle.class;
            }
            if (hasResolverRelationships) {
                paramTypes[argIndex] = Map.class;
            }

            Implementation.Composable ctorCall;
            try {
                ctorCall = MethodCall.invoke(Object.class.getConstructor());
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to resolve Object constructor", e);
            }
            argIndex = 0;
            for (var converterField : converterFields) {
                ctorCall = ctorCall.andThen(FieldAccessor.ofField(converterField.converterFieldName)
                        .setsArgumentAt(argIndex++));
            }
            for (int i = 0; i < planFields.size(); i++) {
                ctorCall = ctorCall.andThen(FieldAccessor.ofField(planFieldName(i)).setsArgumentAt(argIndex++));
            }
            for (int i = 0; i < relationshipFields.size(); i++) {
                ctorCall = ctorCall.andThen(FieldAccessor.ofField(idHandleFieldName(i)).setsArgumentAt(argIndex++));
            }
            if (hasResolverRelationships) {
                ctorCall = ctorCall.andThen(FieldAccessor.ofField(RELATIONSHIP_HANDLE_MAP_FIELD)
                        .setsArgumentAt(argIndex));
            }

            builder = builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(paramTypes)
                    .intercept(ctorCall);
        }

        builder = builder.defineMethod("save", Object.class, Visibility.PUBLIC)
                .withParameters(Object.class, GeneratedTable.class, Object.class)
                .intercept(new Implementation.Simple(
                        new SaveAppender(entityClass, fields, idField, idColumnIndex, columnCount)));

        builder = builder.defineMethod("extractId", Object.class, Visibility.PUBLIC)
                .withParameters(Object.class)
                .intercept(new Implementation.Simple(new ExtractIdAppender(entityClass, idField)));

        builder = builder.defineMethod("setId", void.class, Visibility.PUBLIC)
                .withParameters(Object.class, Object.class)
                .intercept(new Implementation.Simple(new SetIdAppender(entityClass, idField)));

        builder = builder.defineMethod("resolveRelationshipId", Object.class, Visibility.PUBLIC)
                .withParameters(String.class, Object.class)
                .intercept(new Implementation.Simple(new ResolveRelationshipAppender(hasResolverRelationships)));

        try (var unloaded = builder.make()) {
            var implClass = unloaded.load(entityClass.getClassLoader()).getLoaded();
            if (totalParams == 0) {
                @SuppressWarnings("unchecked")
                var saver = (EntitySaver<T, ?>) implClass.getDeclaredConstructor().newInstance();
                return saver;
            }

            var args = new Object[totalParams];
            var paramTypes = new Class<?>[totalParams];
            int argIndex = 0;
            for (var converterField : converterFields) {
                args[argIndex] = converterField.converter;
                paramTypes[argIndex] = TypeConverter.class;
                argIndex++;
            }
            for (var planField : planFields) {
                args[argIndex] = planField.plan;
                paramTypes[argIndex] = ColumnAccessPlan.class;
                argIndex++;
            }

            for (var relationshipField : relationshipFields) {
                var handle = resolverRelationshipHandles.get(relationshipField.mapping.name());
                if (handle == null) {
                    handle = createRelationshipIdHandle(relationshipField.targetEntityClass);
                }
                args[argIndex] = handle;
                paramTypes[argIndex] = MethodHandle.class;
                argIndex++;
            }

            if (hasResolverRelationships) {
                args[argIndex] = resolverRelationshipHandles;
                paramTypes[argIndex] = Map.class;
            }

            @SuppressWarnings("unchecked")
            var saver = (EntitySaver<T, ?>) implClass.getDeclaredConstructor(paramTypes).newInstance(args);
            return saver;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate saver for " + entityClass.getName(), e);
        }
    }

    private static MethodHandle createRelationshipIdHandle(Class<?> entityClass) {
        var idField = findAnnotatedIdField(entityClass);
        if (idField == null) {
            throw new IllegalStateException("Missing explicit @Id field on " + entityClass.getName());
        }
        try {
            if (!Modifier.isPublic(idField.getModifiers())) {
                idField.setAccessible(true);
            }
            return MethodHandles.lookup().unreflectGetter(idField);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to create relationship ID accessor for " + entityClass.getName(), e);
        }
    }

    private static Map<String, MethodHandle> resolveRelationshipHandles(Class<?> entityClass, EntityMetadata<?> metadata) {
        var handles = new HashMap<String, MethodHandle>();
        for (var field : entityClass.getDeclaredFields()) {
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            if (field.getType().isPrimitive()
                    || field.getType() == String.class
                    || field.getType().isArray()
                    || java.util.Collection.class.isAssignableFrom(field.getType())
                    || java.util.Map.class.isAssignableFrom(field.getType())) {
                continue;
            }

            Class<?> targetEntity = null;
            for (var mapping : metadata.fields()) {
                if (!field.getName().equals(mapping.name())) {
                    continue;
                }
                if (mapping.isRelationship() && !mapping.isCollection()) {
                    targetEntity = mapping.targetEntity() != null ? mapping.targetEntity() : field.getType();
                }
                break;
            }
            if (targetEntity == null || targetEntity == void.class) {
                targetEntity = field.getType();
            }

            var idField = findAnnotatedIdField(targetEntity);
            if (idField == null) {
                continue;
            }
            try {
                if (!Modifier.isPublic(idField.getModifiers())) {
                    idField.setAccessible(true);
                }
                handles.put(field.getName(), MethodHandles.lookup().unreflectGetter(idField));
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to create relationship ID accessor for " + targetEntity.getName(), e);
            }
        }
        return handles.isEmpty() ? Map.of() : Map.copyOf(handles);
    }

    private static List<FieldInfo> resolveFields(Class<?> entityClass, EntityMetadata<?> metadata, String idFieldName) {
        List<FieldInfo> result = new ArrayList<>();
        for (FieldMapping mapping : metadata.fields()) {
            if (mapping.columnPosition() < 0 || mapping.name().equals(idFieldName)) {
                continue;
            }

            TypeConverter<?, ?> converter = resolveConverter(entityClass, metadata, mapping.name());
            if (TypeConverterRegistry.isNoOpConverter(converter)) {
                converter = null;
            }

            Field directField = resolveDirectField(entityClass, mapping.name());
            ColumnAccessPlan plan = directField == null ? metadata.columnAccessPlan(mapping.name()) : null;

            Class<?> targetEntityClass = null;
            if (mapping.isRelationship() && !mapping.isCollection()) {
                targetEntityClass = mapping.targetEntity();
                if ((targetEntityClass == null || targetEntityClass == void.class) && directField != null) {
                    targetEntityClass = directField.getType();
                }
                if ((targetEntityClass == null || targetEntityClass == void.class)
                        && mapping.javaType() != null
                        && !mapping.javaType().isPrimitive()) {
                    targetEntityClass = mapping.javaType();
                }
            }
            result.add(new FieldInfo(mapping, directField, plan, converter, targetEntityClass));
        }
        result.sort(Comparator.comparingInt(fieldInfo -> fieldInfo.mapping.columnPosition()));
        return result;
    }

    private static Field resolveDirectField(Class<?> entityClass, String propertyPath) {
        if (propertyPath == null || propertyPath.indexOf('.') >= 0) {
            return null;
        }
        try {
            var field = entityClass.getDeclaredField(propertyPath);
            return Modifier.isPublic(field.getModifiers()) ? field : null;
        } catch (NoSuchFieldException ignored) {
            return null;
        }
    }

    private static TypeConverter<?, ?> resolveConverter(Class<?> entityClass, EntityMetadata<?> metadata, String property) {
        var fieldConverter = TypeConverterRegistry.getInstance().getFieldConverter(entityClass, property);
        if (fieldConverter != null) {
            return fieldConverter;
        }
        return metadata.converters().get(property);
    }

    private static FieldInfo resolveIdField(Class<?> entityClass, EntityMetadata<?> metadata) {
        var idFieldName = metadata.idColumnName();
        try {
            var field = entityClass.getDeclaredField(idFieldName);
            if (!Modifier.isPublic(field.getModifiers())) {
                throw new RuntimeException("ID field must be public: " + idFieldName);
            }
            for (FieldMapping mapping : metadata.fields()) {
                if (mapping.name().equals(idFieldName)) {
                    return new FieldInfo(mapping, field, null, null, null);
                }
            }
            return new FieldInfo(null, field, null, null, null);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("ID field not found: " + idFieldName, e);
        }
    }

    private static void assignConverterFieldNames(List<FieldInfo> converterFields) {
        var names = new HashMap<String, Integer>();
        for (var converterField : converterFields) {
            String sourceName = converterField.mapping != null ? converterField.mapping.name() : converterField.field.getName();
            var baseName = converterFieldNameForField(sourceName);
            var index = names.getOrDefault(baseName, 0);
            var resolvedName = index == 0 ? baseName : baseName + index;
            names.put(baseName, index + 1);
            converterField.converterFieldName = resolvedName;
        }
    }

    private static String converterFieldNameForField(String fieldName) {
        if (fieldName == null || fieldName.isBlank()) {
            return "fieldConverter";
        }
        var first = fieldName.charAt(0);
        var builder = new StringBuilder(fieldName.length() + 10);
        builder.append(Character.isJavaIdentifierStart(first) ? first : '_');
        for (var i = 1; i < fieldName.length(); i++) {
            var current = fieldName.charAt(i);
            builder.append(Character.isJavaIdentifierPart(current) ? current : '_');
        }
        builder.append("Converter");
        return builder.toString();
    }

    private static String planFieldName(int index) {
        return "p" + index;
    }

    private static String idHandleFieldName(int index) {
        return "mh" + index;
    }

    private static final class FieldInfo {
        final FieldMapping mapping;
        final Field field;
        final ColumnAccessPlan plan;
        final TypeConverter<?, ?> converter;
        final Class<?> targetEntityClass;
        String converterFieldName;
        int planIndex = -1;
        int idHandleIndex = -1;

        FieldInfo(FieldMapping mapping, Field field, ColumnAccessPlan plan, TypeConverter<?, ?> converter,
                Class<?> targetEntityClass) {
            this.mapping = mapping;
            this.field = field;
            this.plan = plan;
            this.converter = converter;
            this.targetEntityClass = targetEntityClass;
        }

        boolean hasDirectField() {
            return field != null;
        }

        boolean isRelationship() {
            return mapping != null
                    && mapping.isRelationship()
                    && !mapping.isCollection()
                    && targetEntityClass != null;
        }
    }

    private record SaveAppender(Class<?> entityClass,
            List<FieldInfo> fields,
            FieldInfo idField,
            int idColumnIndex,
            int columnCount) implements ByteCodeAppender {

        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context,
                MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String saverInternal = context.getInstrumentedType().getInternalName();
            String tableInternal = Type.getInternalName(GeneratedTable.class);

            int valuesVar = 4;
            int entityVar = 5;
            int objVar = 9;

            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, entityInternal);
            mv.visitVarInsn(Opcodes.ASTORE, entityVar);

            String idFieldDescriptor = Type.getDescriptor(idField.field.getType());
            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(idField.field.getType()));
            mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, idField.field.getName(), idFieldDescriptor);

            pushInt(mv, columnCount);
            mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
            mv.visitVarInsn(Opcodes.ASTORE, valuesVar);

            mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
            pushInt(mv, idColumnIndex);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitInsn(Opcodes.AASTORE);

            for (FieldInfo info : fields) {
                emitFieldToArray(mv, info, info.mapping.columnPosition(), saverInternal, entityInternal,
                        entityVar, valuesVar, objVar);
            }

            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, tableInternal, "insertFrom", "([Ljava/lang/Object;)J", true);
            mv.visitInsn(Opcodes.POP2);

            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitInsn(Opcodes.ARETURN);

            return new Size(8, objVar + 1);
        }

        private void emitFieldToArray(MethodVisitor mv, FieldInfo info, int arrayIndex,
                String saverInternal, String entityInternal,
                int entityVar, int valuesVar, int objVar) {
            if (info.hasDirectField() && !info.isRelationship()) {
                emitDirectFieldToArray(mv, info, arrayIndex, saverInternal, entityInternal, entityVar, valuesVar);
                return;
            }

            if (info.hasDirectField()) {
                mv.visitVarInsn(Opcodes.ALOAD, entityVar);
                mv.visitFieldInsn(Opcodes.GETFIELD, entityInternal, info.field.getName(),
                        Type.getDescriptor(info.field.getType()));
                mv.visitVarInsn(Opcodes.ASTORE, objVar);
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, saverInternal, planFieldName(info.planIndex),
                        "Lio/memris/core/ColumnAccessPlan;");
                mv.visitVarInsn(Opcodes.ALOAD, entityVar);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntitySaverGenerator.class),
                        "readWithPlan",
                        "(Lio/memris/core/ColumnAccessPlan;Ljava/lang/Object;)Ljava/lang/Object;",
                        false);
                mv.visitVarInsn(Opcodes.ASTORE, objVar);
            }

            if (info.isRelationship()) {
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, saverInternal, idHandleFieldName(info.idHandleIndex),
                        "Ljava/lang/invoke/MethodHandle;");
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntitySaverGenerator.class),
                        "invokeRelationshipIdGetter",
                        "(Ljava/lang/invoke/MethodHandle;Ljava/lang/Object;)Ljava/lang/Object;",
                        false);
                mv.visitVarInsn(Opcodes.ASTORE, objVar);
            }

            if (info.converter != null) {
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, saverInternal, info.converterFieldName,
                        "Lio/memris/core/converter/TypeConverter;");
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntitySaverGenerator.class),
                        "convertToStorage",
                        "(Lio/memris/core/converter/TypeConverter;Ljava/lang/Object;)Ljava/lang/Object;",
                        false);
                mv.visitVarInsn(Opcodes.ASTORE, objVar);
            }

            mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
            pushInt(mv, arrayIndex);
            mv.visitVarInsn(Opcodes.ALOAD, objVar);
            mv.visitInsn(Opcodes.AASTORE);
        }

        private void emitDirectFieldToArray(MethodVisitor mv, FieldInfo info, int arrayIndex, String saverInternal,
                String entityInternal, int entityVar, int valuesVar) {
            Field field = info.field;
            Class<?> fieldType = field.getType();

            mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
            pushInt(mv, arrayIndex);
            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitFieldInsn(Opcodes.GETFIELD, entityInternal, field.getName(), Type.getDescriptor(fieldType));

            if (info.converter != null) {
                if (fieldType.isPrimitive()) {
                    boxPrimitive(mv, fieldType);
                }
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, saverInternal, info.converterFieldName,
                        "Lio/memris/core/converter/TypeConverter;");
                mv.visitInsn(Opcodes.SWAP);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntitySaverGenerator.class),
                        "convertToStorage",
                        "(Lio/memris/core/converter/TypeConverter;Ljava/lang/Object;)Ljava/lang/Object;",
                        false);
            } else if (fieldType.isPrimitive()) {
                boxPrimitive(mv, fieldType);
            }
            mv.visitInsn(Opcodes.AASTORE);
        }

        private void boxPrimitive(MethodVisitor mv, Class<?> primitiveType) {
            if (primitiveType == int.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;",
                        false);
            } else if (primitiveType == long.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
            } else if (primitiveType == boolean.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;",
                        false);
            } else if (primitiveType == byte.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false);
            } else if (primitiveType == short.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false);
            } else if (primitiveType == char.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Character", "valueOf", "(C)Ljava/lang/Character;",
                        false);
            } else if (primitiveType == float.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false);
            } else if (primitiveType == double.class) {
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;",
                        false);
            }
        }
    }

    private record ExtractIdAppender(Class<?> entityClass, FieldInfo idField) implements ByteCodeAppender {
        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context, MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String idFieldDescriptor = Type.getDescriptor(idField.field.getType());
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, entityInternal);
            mv.visitFieldInsn(Opcodes.GETFIELD, entityInternal, idField.field.getName(), idFieldDescriptor);
            mv.visitInsn(Opcodes.ARETURN);
            return new Size(2, 2);
        }
    }

    private record SetIdAppender(Class<?> entityClass, FieldInfo idField) implements ByteCodeAppender {
        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context, MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String idFieldDescriptor = Type.getDescriptor(idField.field.getType());
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, entityInternal);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(idField.field.getType()));
            mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, idField.field.getName(), idFieldDescriptor);
            mv.visitInsn(Opcodes.RETURN);
            return new Size(3, 3);
        }
    }

    private record ResolveRelationshipAppender(boolean hasRelationships) implements ByteCodeAppender {
        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context, MethodDescription method) {
            if (!hasRelationships) {
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitInsn(Opcodes.ARETURN);
                return new Size(1, 3);
            }
            String ownerInternal = context.getInstrumentedType().getInternalName();
            mv.visitVarInsn(Opcodes.ALOAD, 0);
            mv.visitFieldInsn(Opcodes.GETFIELD, ownerInternal, RELATIONSHIP_HANDLE_MAP_FIELD, "Ljava/util/Map;");
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                    Type.getInternalName(EntitySaverGenerator.class),
                    "resolveRelationshipIdFromHandles",
                    "(Ljava/util/Map;Ljava/lang/String;Ljava/lang/Object;)Ljava/lang/Object;",
                    false);
            mv.visitInsn(Opcodes.ARETURN);
            return new Size(3, 3);
        }
    }

    private static void pushInt(MethodVisitor mv, int value) {
        if (value == -1) {
            mv.visitInsn(Opcodes.ICONST_M1);
        } else if (value >= 0 && value <= 5) {
            mv.visitInsn(Opcodes.ICONST_0 + value);
        } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
            mv.visitIntInsn(Opcodes.BIPUSH, value);
        } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
            mv.visitIntInsn(Opcodes.SIPUSH, value);
        } else {
            mv.visitLdcInsn(value);
        }
    }

    public static Object convertToStorage(TypeConverter<?, ?> converter, Object value) {
        if (value == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        TypeConverter<Object, Object> typed = (TypeConverter<Object, Object>) converter;
        return typed.toStorage(value);
    }

    public static Object readWithPlan(ColumnAccessPlan plan, Object root) {
        if (plan == null || root == null) {
            return null;
        }
        return plan.get(root);
    }

    public static Object invokeRelationshipIdGetter(MethodHandle mh, Object relatedEntity) {
        if (mh == null || relatedEntity == null) {
            return null;
        }
        try {
            return mh.invoke(relatedEntity);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get relationship ID", t);
        }
    }

    public static Object resolveRelationshipIdFromHandles(Map<String, MethodHandle> relationshipHandles, String fieldName,
            Object relatedEntity) {
        if (relationshipHandles == null || fieldName == null || relatedEntity == null) {
            return null;
        }
        var handle = relationshipHandles.get(fieldName);
        return invokeRelationshipIdGetter(handle, relatedEntity);
    }

    private static Field findAnnotatedIdField(Class<?> entityClass) {
        Field resolved = null;
        for (var field : entityClass.getDeclaredFields()) {
            if (!field.isAnnotationPresent(Id.class)) {
                continue;
            }
            if (resolved != null) {
                throw new IllegalArgumentException("Multiple ID fields found in " + entityClass.getName());
            }
            resolved = field;
        }
        return resolved;
    }

    private static final class ShapeKey {
        private final ClassLoader classLoader;
        private final Class<?> entityClass;
        private final String columnSignature;
        private final String converterSignature;

        private ShapeKey(ClassLoader classLoader, Class<?> entityClass, String columnSignature,
                String converterSignature) {
            this.classLoader = classLoader;
            this.entityClass = entityClass;
            this.columnSignature = columnSignature;
            this.converterSignature = converterSignature;
        }

        static ShapeKey from(EntityMetadata<?> metadata) {
            var fields = metadata.fields().stream()
                    .filter(mapping -> mapping.columnPosition() >= 0)
                    .sorted(Comparator.comparingInt(FieldMapping::columnPosition))
                    .toList();
            var columnBuilder = new StringBuilder(fields.size() * 56);
            var converterBuilder = new StringBuilder(fields.size() * 32);
            for (var mapping : fields) {
                columnBuilder.append(mapping.columnPosition())
                        .append(':')
                        .append(mapping.name())
                        .append(':')
                        .append(mapping.typeCode())
                        .append(':')
                        .append(mapping.javaType().getName())
                        .append(':')
                        .append(mapping.isRelationship())
                        .append(':')
                        .append(mapping.isEmbedded())
                        .append(':')
                        .append(mapping.targetEntity() != null ? mapping.targetEntity().getName() : "-")
                        .append('|');
                var converter = resolveConverter(metadata.entityClass(), metadata, mapping.name());
                if (TypeConverterRegistry.isNoOpConverter(converter)) {
                    converter = null;
                }
                converterBuilder.append(mapping.columnPosition())
                        .append(':')
                        .append(System.identityHashCode(converter))
                        .append('|');
            }
            return new ShapeKey(
                    metadata.entityClass().getClassLoader(),
                    metadata.entityClass(),
                    columnBuilder.toString(),
                    converterBuilder.toString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ShapeKey other)) {
                return false;
            }
            return classLoader == other.classLoader
                    && entityClass.equals(other.entityClass)
                    && columnSignature.equals(other.columnSignature)
                    && converterSignature.equals(other.converterSignature);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    System.identityHashCode(classLoader),
                    entityClass,
                    columnSignature,
                    converterSignature);
        }
    }
}

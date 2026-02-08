package io.memris.repository;

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
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Generates EntitySaver implementations using ByteBuddy with zero reflection.
 * <p>
 * All generated classes use direct field access - no MethodHandle fields.
 * This eliminates MethodHandle.invoke() overhead during entity save operations.
 */
public final class EntitySaverGenerator {

    /**
     * Generate an EntitySaver implementation for the given entity class and
     * metadata.
     *
     * @param entityClass the entity class
     * @param metadata    the entity metadata
     * @return a generated EntitySaver instance
     *
     * Equivalent generated Java (simplified):
     *
     * <pre>{@code
     * final class Customer$MemrisSaver$123 implements EntitySaver<Customer, Object> {
     *     private final TypeConverter<?, ?> c0;
     *     private final MethodHandle relId0;
     *
     *     public Object save(Object entity, GeneratedTable table, Object id) {
     *         // materialize storage array, then table.insertFrom(values)
     *         // return assigned id
     *     }
     *
     *     public Object extractId(Object entity) { ... }
     *     public void setId(Object entity, Object id) { ... }
     *     public Object resolveRelationshipId(String field, Object entity) { ... }
     * }
     * }</pre>
     */
    public static <T> EntitySaver<T, ?> generate(Class<T> entityClass, EntityMetadata<T> metadata) {
        var idField = resolveIdField(entityClass, metadata);
        var fields = resolveFields(entityClass, metadata, idField.field.getName());
        var converterFields = fields.stream().filter(f -> f.converter != null).toList();
        var relationshipFields = fields.stream().filter(FieldInfo::isRelationship).toList();
        var relationships = resolveRelationships(entityClass, metadata);

        // Assign indices to relationship fields for MethodHandle field access
        for (int i = 0; i < relationshipFields.size(); i++) {
            relationshipFields.get(i).idHandleIndex = i;
        }

        var implName = entityClass.getName() + "$MemrisSaver$" + System.nanoTime();
        var builder = new ByteBuddy()
                .subclass(Object.class)
                .implement(EntitySaver.class)
                .name(implName)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        // Define converter fields
        for (int i = 0; i < converterFields.size(); i++) {
            builder = builder.defineField(converterFieldName(i), TypeConverter.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }

        // Define MethodHandle fields for relationship ID extraction (ZERO REFLECTION on
        // hot path!)
        for (int i = 0; i < relationshipFields.size(); i++) {
            builder = builder.defineField(idHandleFieldName(i), MethodHandle.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }

        // Build constructor parameter types: converters first, then MethodHandles
        var totalParams = converterFields.size() + relationshipFields.size();
        if (totalParams > 0) {
            var paramTypes = new Class<?>[totalParams];
            for (int i = 0; i < converterFields.size(); i++) {
                paramTypes[i] = TypeConverter.class;
                converterFields.get(i).converterIndex = i;
            }
            for (int i = 0; i < relationshipFields.size(); i++) {
                paramTypes[converterFields.size() + i] = MethodHandle.class;
            }

            Implementation.Composable ctorCall;
            try {
                ctorCall = MethodCall.invoke(Object.class.getConstructor());
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to resolve Object constructor", e);
            }
            // Set converter fields
            for (int i = 0; i < converterFields.size(); i++) {
                ctorCall = ctorCall.andThen(FieldAccessor.ofField(converterFieldName(i)).setsArgumentAt(i));
            }
            // Set MethodHandle fields
            for (int i = 0; i < relationshipFields.size(); i++) {
                ctorCall = ctorCall.andThen(FieldAccessor.ofField(idHandleFieldName(i))
                        .setsArgumentAt(converterFields.size() + i));
            }

            builder = builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(paramTypes)
                    .intercept(ctorCall);
        } else {
            // Default no-arg constructor
            try {
                builder = builder.defineConstructor(Visibility.PUBLIC)
                        .withParameters(new Class<?>[0])
                        .intercept(MethodCall.invoke(Object.class.getConstructor()));
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to resolve Object constructor", e);
            }
        }

        // Define save() method - uses Object for ID to support any ID type
        builder = builder.defineMethod("save", Object.class, Visibility.PUBLIC)
                .withParameters(Object.class, io.memris.storage.GeneratedTable.class, Object.class)
                .intercept(new Implementation.Simple(new SaveAppender(entityClass, fields, idField)));

        // Define extractId() method - returns Object for generic ID type
        builder = builder.defineMethod("extractId", Object.class, Visibility.PUBLIC)
                .withParameters(Object.class)
                .intercept(new Implementation.Simple(new ExtractIdAppender(entityClass, idField)));

        // Define setId() method - takes Object for generic ID type
        builder = builder.defineMethod("setId", void.class, Visibility.PUBLIC)
                .withParameters(Object.class, Object.class)
                .intercept(new Implementation.Simple(new SetIdAppender(entityClass, idField)));

        // Define resolveRelationshipId() method
        builder = builder.defineMethod("resolveRelationshipId", Object.class, Visibility.PUBLIC)
                .withParameters(String.class, Object.class)
                .intercept(new Implementation.Simple(new ResolveRelationshipAppender(relationships)));

        // Build and load the class
        try (var unloaded = builder.make()) {
            var implClass = unloaded.load(entityClass.getClassLoader()).getLoaded();

            var totalArgs = converterFields.size() + relationshipFields.size();
            if (totalArgs == 0) {
                @SuppressWarnings("unchecked")
                var saver = (EntitySaver<T, ?>) implClass.getDeclaredConstructor().newInstance();
                return saver;
            }

            var args = new Object[totalArgs];
            var paramTypes = new Class<?>[totalArgs];

            // Populate converter args
            for (int i = 0; i < converterFields.size(); i++) {
                args[i] = converterFields.get(i).converter;
                paramTypes[i] = TypeConverter.class;
            }

            // Create MethodHandles for relationship ID extraction (reflection happens ONCE
            // here)
            var lookup = MethodHandles.lookup();
            for (int i = 0; i < relationshipFields.size(); i++) {
                var rf = relationshipFields.get(i);
                try {
                    var targetIdField = findAnnotatedIdField(rf.targetEntityClass);
                    if (targetIdField == null) {
                        throw new IllegalStateException("Missing explicit @Id field on "
                                + rf.targetEntityClass.getName());
                    }
                    if (!Modifier.isPublic(targetIdField.getModifiers())) {
                        targetIdField.setAccessible(true);
                    }
                    var mh = lookup.unreflectGetter(targetIdField);
                    args[converterFields.size() + i] = mh;
                    paramTypes[converterFields.size() + i] = MethodHandle.class;
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to create MethodHandle for " + rf.field.getName(), e);
                }
            }

            @SuppressWarnings("unchecked")
            var saver = (EntitySaver<T, ?>) implClass.getDeclaredConstructor(paramTypes)
                    .newInstance(args);
            return saver;
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate saver for " + entityClass.getName(), e);
        }
    }

    private static List<FieldInfo> resolveFields(Class<?> entityClass, EntityMetadata<?> metadata, String idFieldName) {
        List<FieldInfo> result = new ArrayList<>();
        for (FieldMapping mapping : metadata.fields()) {
            if (mapping.columnPosition() < 0) {
                continue;
            }
            // Skip the ID field - it's handled separately
            if (mapping.name().equals(idFieldName)) {
                continue;
            }
            try {
                var field = entityClass.getDeclaredField(mapping.name());
                if (!Modifier.isPublic(field.getModifiers())) {
                    continue;
                }
                TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance()
                        .getFieldConverter(entityClass, mapping.name());
                if (converter == null) {
                    converter = metadata.converters().get(mapping.name());
                }

                // For relationship fields, resolve the target entity class
                Class<?> targetEntityClass = null;
                if (mapping.isRelationship()) {
                    targetEntityClass = mapping.targetEntity();
                    if (targetEntityClass == null || targetEntityClass == void.class) {
                        targetEntityClass = field.getType();
                    }
                }

                result.add(new FieldInfo(mapping, field, converter, targetEntityClass));
            } catch (NoSuchFieldException ignored) {
                // Skip fields not present
            }
        }
        return result;
    }

    private static FieldInfo resolveIdField(Class<?> entityClass, EntityMetadata<?> metadata) {
        var idFieldName = metadata.idColumnName();
        try {
            var field = entityClass.getDeclaredField(idFieldName);
            if (!Modifier.isPublic(field.getModifiers())) {
                throw new RuntimeException("ID field must be public: " + idFieldName);
            }
            // Find the FieldMapping for the ID field
            for (FieldMapping mapping : metadata.fields()) {
                if (mapping.name().equals(idFieldName)) {
                    return new FieldInfo(mapping, field, null);
                }
            }
            // If not found in mappings, create a simple one
            return new FieldInfo(null, field, null);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("ID field not found: " + idFieldName, e);
        }
    }

    private static Map<String, RelationshipInfo> resolveRelationships(Class<?> entityClass,
            EntityMetadata<?> metadata) {
        Map<String, RelationshipInfo> result = new HashMap<>();

        // First, handle fields marked as relationships in metadata
        for (FieldMapping mapping : metadata.fields()) {
            if (mapping.isRelationship()) {
                try {
                    var field = entityClass.getDeclaredField(mapping.name());
                    if (!Modifier.isPublic(field.getModifiers())) {
                        continue;
                    }
                    // Find the ID field in the target entity class
                    var targetEntity = mapping.targetEntity();
                    if (targetEntity == null) {
                        targetEntity = field.getType();
                    }
                    var targetIdField = findIdField(targetEntity);
                    if (targetIdField != null) {
                        result.put(mapping.name(), new RelationshipInfo(mapping, field, targetEntity, targetIdField));
                    }
                } catch (NoSuchFieldException ignored) {
                    // Skip if field not found
                }
            }
        }

        // Also handle entity reference fields (fields whose type has an 'id' field)
        // This supports test entities without relationship annotations
        for (FieldMapping mapping : metadata.fields()) {
            if (!mapping.isRelationship() && !result.containsKey(mapping.name())) {
                try {
                    var field = entityClass.getDeclaredField(mapping.name());
                    if (!Modifier.isPublic(field.getModifiers())) {
                        continue;
                    }
                    var fieldType = field.getType();
                    // Skip primitive types and common non-entity types
                    if (fieldType.isPrimitive() ||
                            fieldType == String.class ||
                            fieldType.isArray() ||
                            Collection.class.isAssignableFrom(fieldType) ||
                            Map.class.isAssignableFrom(fieldType)) {
                        continue;
                    }
                    // Check if the field type has an 'id' field (indicating it's an entity)
                    var targetIdField = findIdField(fieldType);
                    if (targetIdField != null) {
                        // Create a synthetic relationship info
                        result.put(mapping.name(), new RelationshipInfo(mapping, field, fieldType, targetIdField));
                    }
                } catch (NoSuchFieldException ignored) {
                    // Skip if field not found
                }
            }
        }

        return result;
    }

    private static Field findIdField(Class<?> entityClass) {
        Field resolved = null;
        for (var field : entityClass.getDeclaredFields()) {
            if (!isIdField(field) || !Modifier.isPublic(field.getModifiers())) {
                continue;
            }
            if (resolved != null) {
                throw new IllegalArgumentException("Multiple public ID fields found in " + entityClass.getName());
            }
            resolved = field;
        }
        return resolved;
    }

    private static boolean isIdField(Field field) {
        return field.isAnnotationPresent(Id.class);
    }

    private static String converterFieldName(int index) {
        return "c" + index;
    }

    private static String idHandleFieldName(int index) {
        return "mh" + index; // MethodHandle for relationship id extraction
    }

    private static final class FieldInfo {
        final FieldMapping mapping;
        final Field field;
        final TypeConverter<?, ?> converter;
        final Class<?> targetEntityClass; // For relationship fields, the related entity's class
        int converterIndex = -1;
        int idHandleIndex = -1; // For relationship fields, index of the MethodHandle field for id extraction

        FieldInfo(FieldMapping mapping, Field field, TypeConverter<?, ?> converter) {
            this(mapping, field, converter, null);
        }

        FieldInfo(FieldMapping mapping, Field field, TypeConverter<?, ?> converter, Class<?> targetEntityClass) {
            this.mapping = mapping;
            this.field = field;
            this.converter = converter;
            this.targetEntityClass = targetEntityClass;
        }

        boolean isRelationship() {
            return mapping != null && mapping.isRelationship() && targetEntityClass != null;
        }
    }

    private record RelationshipInfo(FieldMapping mapping, Field field, Class<?> targetEntityClass,
            Field targetIdField) {
    }

    /**
     * Bytecode appender for the save() method.
     */
    private record SaveAppender(Class<?> entityClass, List<FieldInfo> fields,
            FieldInfo idField) implements ByteCodeAppender {

        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context,
                MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String saverInternal = context.getInstrumentedType().getInternalName();
            String tableInternal = Type.getInternalName(GeneratedTable.class);
            // Variables:
            // 1 = entity (Object)
            // 2 = table (GeneratedTable)
            // 3 = id (Long)
            // 4 = values array (Object[])
            // 5 = entity (casted)
            int valuesVar = 4;
            int entityVar = 5;
            int objVar = 9;

            // Cast entity parameter to actual type
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, entityInternal);
            mv.visitVarInsn(Opcodes.ASTORE, entityVar);

            // Set ID on entity FIRST: entity.id = id
            // Cast the id parameter to the actual field type
            String idFieldDescriptor = Type.getDescriptor(idField.field.getType());
            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitVarInsn(Opcodes.ALOAD, 3);
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(idField.field.getType()));
            mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, idField.field.getName(), idFieldDescriptor);

            // Create values array: new Object[columnCount + 1] (including ID at index 0)
            int columnCount = fields.size() + 1; // +1 for ID field
            pushInt(mv, columnCount);
            mv.visitTypeInsn(Opcodes.ANEWARRAY, "java/lang/Object");
            mv.visitVarInsn(Opcodes.ASTORE, valuesVar);

            // Store ID at index 0
            mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
            pushInt(mv, 0);
            mv.visitVarInsn(Opcodes.ALOAD, 3); // Load the id parameter
            mv.visitInsn(Opcodes.AASTORE);

            // For each field: extract value from entity, convert if needed, store in array
            // (starting at index 1)
            for (int i = 0; i < fields.size(); i++) {
                FieldInfo info = fields.get(i);
                emitFieldToArray(mv, info, i + 1, saverInternal, entityInternal,
                        entityVar, valuesVar, objVar);
            }

            // Call table.insertFrom(values)
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, tableInternal, "insertFrom", "([Ljava/lang/Object;)J", true);
            // Pop the return value (packed ref)
            mv.visitInsn(Opcodes.POP2);

            // Return the entity
            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitInsn(Opcodes.ARETURN);

            return new Size(8, objVar + 1);
        }

        private void emitFieldToArray(MethodVisitor mv, FieldInfo info, int arrayIndex,
                String saverInternal, String entityInternal,
                int entityVar, int valuesVar, int objVar) {
            Field field = info.field;
            Class<?> fieldType = field.getType();

            // Check if this is a relationship field - if so, use cached MethodHandle
            // COMPILE ONCE, REUSE FOREVER - no reflection on hot path!
            if (info.isRelationship()) {
                // Load entity.customer into objVar
                mv.visitVarInsn(Opcodes.ALOAD, entityVar);
                mv.visitFieldInsn(Opcodes.GETFIELD, entityInternal, field.getName(), Type.getDescriptor(fieldType));
                mv.visitVarInsn(Opcodes.ASTORE, objVar); // objVar = entity.customer

                // Invoke MethodHandle to get id: invokeRelationshipIdGetter(mh, relatedEntity)
                // Load the MethodHandle from this.mhX field
                mv.visitVarInsn(Opcodes.ALOAD, 0); // this
                mv.visitFieldInsn(Opcodes.GETFIELD, saverInternal, idHandleFieldName(info.idHandleIndex),
                        "Ljava/lang/invoke/MethodHandle;");
                // Load the related entity
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                // Call static helper: invokeRelationshipIdGetter(MethodHandle, Object) -> Long
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntitySaverGenerator.class),
                        "invokeRelationshipIdGetter",
                        "(Ljava/lang/invoke/MethodHandle;Ljava/lang/Object;)Ljava/lang/Long;",
                        false);
                mv.visitVarInsn(Opcodes.ASTORE, objVar); // objVar now has the Long id (or null)

                // Now do the normal array store: values[index] = objVar
                mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
                pushInt(mv, arrayIndex);
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                mv.visitInsn(Opcodes.AASTORE);
                return; // We've already stored, return early
            }

            // Non-relationship field - load values array and index, then value
            mv.visitVarInsn(Opcodes.ALOAD, valuesVar);
            pushInt(mv, arrayIndex);

            // Load entity field value directly
            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitFieldInsn(Opcodes.GETFIELD, entityInternal, field.getName(), Type.getDescriptor(fieldType));

            // Handle converter if present
            if (info.converter != null) {
                // Box primitive if needed
                if (fieldType.isPrimitive()) {
                    boxPrimitive(mv, fieldType);
                }
                // Call converter.toStorage()
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, saverInternal, converterFieldName(info.converterIndex),
                        "Lio/memris/core/converter/TypeConverter;");
                mv.visitInsn(Opcodes.SWAP);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntitySaverGenerator.class),
                        "convertToStorage",
                        "(Lio/memris/core/converter/TypeConverter;Ljava/lang/Object;)Ljava/lang/Object;",
                        false);
            } else {
                // No converter - just box if primitive
                if (fieldType.isPrimitive()) {
                    boxPrimitive(mv, fieldType);
                }
            }

            // Store in array
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
                mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;", false);
            }
        }

        private void pushInt(MethodVisitor mv, int value) {
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
    }

    /**
     * Bytecode appender for the extractId() method.
     */
    private record ExtractIdAppender(Class<?> entityClass, FieldInfo idField) implements ByteCodeAppender {

        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context,
                MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String fieldName = idField.field.getName();

            // Load entity parameter
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, entityInternal);

            // Get id field - use actual field type descriptor
            String idFieldDescriptor = Type.getDescriptor(idField.field.getType());
            mv.visitFieldInsn(Opcodes.GETFIELD, entityInternal, fieldName, idFieldDescriptor);

            // Return it
            mv.visitInsn(Opcodes.ARETURN);

            return new Size(2, 2);
        }
    }

    /**
     * Bytecode appender for the setId() method.
     */
    private record SetIdAppender(Class<?> entityClass, FieldInfo idField) implements ByteCodeAppender {

        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context,
                MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String fieldName = idField.field.getName();

            // Load entity parameter
            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitTypeInsn(Opcodes.CHECKCAST, entityInternal);

            // Load id parameter and cast to actual field type
            String idFieldDescriptor = Type.getDescriptor(idField.field.getType());
            mv.visitVarInsn(Opcodes.ALOAD, 2);
            mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(idField.field.getType()));

            // Set id field
            mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, fieldName, idFieldDescriptor);

            // Return
            mv.visitInsn(Opcodes.RETURN);

            return new Size(3, 3);
        }
    }

    /**
     * Bytecode appender for the resolveRelationshipId() method.
     * Uses a simple delegation to a helper method to avoid complex bytecode.
     */
    private record ResolveRelationshipAppender(
            Map<String, RelationshipInfo> relationships) implements ByteCodeAppender {

        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context,
                MethodDescription method) {
            if (relationships.isEmpty()) {
                // No relationships defined - just return null
                mv.visitInsn(Opcodes.ACONST_NULL);
                mv.visitInsn(Opcodes.ARETURN);
                return new Size(1, 3);
            }

            // Build the class name -> relationship mapping string
            // Format: "fieldName1:className1,fieldName2:className2,..."
            var mappingBuilder = new StringBuilder();
            for (var entry : relationships.entrySet()) {
                if (mappingBuilder.length() > 0) {
                    mappingBuilder.append(",");
                }
                mappingBuilder.append(entry.getKey()).append(":").append(entry.getValue().targetEntityClass.getName());
            }
            var relationshipMapping = mappingBuilder.toString();

            // Call helper method: resolveRelationshipIdHelper(fieldName, relatedEntity,
            // relationshipMapping)
            mv.visitVarInsn(Opcodes.ALOAD, 1); // fieldName
            mv.visitVarInsn(Opcodes.ALOAD, 2); // relatedEntity
            mv.visitLdcInsn(relationshipMapping); // relationship mapping
            mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                    Type.getInternalName(EntitySaverGenerator.class),
                    "resolveRelationshipIdHelper",
                    "(Ljava/lang/String;Ljava/lang/Object;Ljava/lang/String;)Ljava/lang/Object;",
                    false);
            mv.visitInsn(Opcodes.ARETURN);

            return new Size(3, 3);
        }

    }

    /**
     * Helper method to convert a value using a TypeConverter.
     */
    public static Object convertToStorage(TypeConverter<?, ?> converter, Object value) {
        if (value == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        TypeConverter<Object, Object> typed = (TypeConverter<Object, Object>) converter;
        return typed.toStorage(value);
    }

    /**
     * Helper to invoke a MethodHandle to get a relationship entity's ID.
     * ZERO REFLECTION - MethodHandle.invoke is as fast as a direct method call.
     * The MethodHandle was created once at saver class generation time.
     */
    public static Long invokeRelationshipIdGetter(MethodHandle mh, Object relatedEntity) {
        if (relatedEntity == null) {
            return null;
        }
        try {
            Object id = mh.invoke(relatedEntity);
            if (id instanceof Long) {
                return (Long) id;
            }
            if (id instanceof Number) {
                return ((Number) id).longValue();
            }
            return null;
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get relationship ID", t);
        }
    }

    /**
     * Helper method to resolve relationship ID.
     * This uses reflection but is only called for relationship resolution (not hot
     * path).
     */
    public static Object resolveRelationshipIdHelper(String fieldName, Object relatedEntity,
            String relationshipMapping) {
        if (relatedEntity == null || fieldName == null) {
            return null;
        }

        // Parse the relationship mapping
        Map<String, Class<?>> fieldToClass = new HashMap<>();
        for (var pair : relationshipMapping.split(",")) {
            var colonIndex = pair.indexOf(':');
            if (colonIndex > 0) {
                var field = pair.substring(0, colonIndex);
                var className = pair.substring(colonIndex + 1);
                try {
                    fieldToClass.put(field, Class.forName(className));
                } catch (ClassNotFoundException e) {
                    // Skip if class not found
                }
            }
        }

        var targetClass = fieldToClass.get(fieldName);
        if (targetClass == null) {
            return null;
        }

        // Find and access the id field
        try {
            var idField = findAnnotatedIdField(targetClass);
            if (idField == null) {
                return null;
            }
            if (!Modifier.isPublic(idField.getModifiers())) {
                idField.setAccessible(true);
            }
            return idField.get(relatedEntity);
        } catch (IllegalAccessException e) {
            return null;
        }
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
}

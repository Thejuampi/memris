package io.memris.core;

import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.EntityMetadata.FieldMapping.RelationshipType;
import io.memris.core.EntityMetadata.IndexDefinition;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

 /**
  * Extracts metadata from entity classes for repository generation.
  * All reflection work happens once during metadata extraction.
  *
  * <p>This class intentionally has high cognitive complexity as it handles:
  * - Multiple relationship types (ManyToOne, OneToOne, OneToMany, ManyToMany)
 * - Memris annotations
  * - Bidirectional relationship discovery
  * - Field-level and class-level metadata extraction
  *
  * <p><b>Complexity Rationale:</b> Reflection is expensive. Doing a single-pass
  * comprehensive extraction per entity type is optimal. Splitting this class
  * would scatter related logic and hurt maintainability.
  *
  * <p><b>Not a hot-path component:</b> This runs only once per entity type during
  * initialization. All hot-path code uses the generated tables via zero-reflection
  * access.
  */
public final class MetadataExtractor {
    /**
     * Extract metadata from an entity class.
     * 
     * @param entityClass the entity class
     * @return the entity metadata
     */
    public static <T> EntityMetadata<T> extractEntityMetadata(Class<T> entityClass) {
        try {
            Constructor<T> constructor = entityClass.getDeclaredConstructor();

            // Find ID field
            String idColumnName = "id";
            for (var field : entityClass.getDeclaredFields()) {
                if (field.getName().equals("id")
                        || field.isAnnotationPresent(GeneratedValue.class)) {
                    idColumnName = field.getName();
                    break;
                }
            }
            
            // Build field mappings with relationship support
            var fields = new ArrayList<FieldMapping>();
            var converters = new HashMap<String, TypeConverter<?, ?>>();
            var indexDefinitions = new ArrayList<IndexDefinition>();
            var foreignKeyColumns = new HashSet<String>();
            var auditFields = new ArrayList<EntityMetadata.AuditField>();
            var colPos = 0;
            
            for (var field : entityClass.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                
                EntityMetadata.AuditFieldType auditType = auditTypeForField(field);
                if (auditType != null) {
                    auditFields.add(new EntityMetadata.AuditField(field.getName(), auditType, field.getType()));
                }

                // Check for relationship annotations
                ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
                OneToOne oneToOne = field.getAnnotation(OneToOne.class);
                OneToMany oneToMany = field.getAnnotation(OneToMany.class);
                ManyToMany manyToMany = field.getAnnotation(ManyToMany.class);
                JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
                JoinTable joinTable = field.getAnnotation(JoinTable.class);
                if (manyToOne != null) {
                    FieldMapping relationshipMapping = createRelationshipMapping(
                        field,
                        manyToOne,
                        joinColumn,
                        colPos,
                        foreignKeyColumns,
                        RelationshipType.MANY_TO_ONE
                    );
                    fields.add(relationshipMapping);
                    colPos++;
                } else if (oneToOne != null) {
                    FieldMapping relationshipMapping = createRelationshipMapping(
                        field,
                        null,
                        joinColumn,
                        colPos,
                        foreignKeyColumns,
                        RelationshipType.ONE_TO_ONE
                    );
                    fields.add(relationshipMapping);
                    colPos++;
                } else if (oneToMany != null) {
                    FieldMapping relationshipMapping = createOneToManyMapping(
                        entityClass,
                        field,
                        oneToMany
                    );
                    fields.add(relationshipMapping);
                } else if (manyToMany != null) {
                    FieldMapping relationshipMapping = createManyToManyMapping(
                        entityClass,
                        field,
                        manyToMany,
                        joinTable
                    );
                    fields.add(relationshipMapping);
                } else {
                    // Regular field
                    TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(field.getType());
                    Class<?> storageType = converter != null ? converter.storageType() : field.getType();
                    byte typeCode = resolveTypeCode(field.getType(), storageType);
                    if (converter != null) {
                        converters.put(field.getName(), converter);
                    }
                    String columnName = field.getName();
                    handleFieldIndexAnnotations(field, colPos, typeCode, indexDefinitions);
                    fields.add(new FieldMapping(
                        field.getName(),
                        columnName,
                        field.getType(),
                        storageType,
                        colPos++,
                        typeCode
                    ));
                }
            }

            handleClassLevelIndexAnnotations(entityClass, fields, indexDefinitions);
            
            // Build MethodHandles for field access (for public fields)
        var fieldGetters = new HashMap<String, MethodHandle>();
        var fieldSetters = new HashMap<String, MethodHandle>();
            
            try {
                java.lang.invoke.MethodHandles.Lookup lookup = java.lang.invoke.MethodHandles.publicLookup();
                
                for (var field : entityClass.getDeclaredFields()) {
                    if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    
                    // Create getter handle for public fields
                    if (java.lang.reflect.Modifier.isPublic(field.getModifiers())) {
                        MethodHandle getter = lookup.unreflectGetter(field);
                        fieldGetters.put(field.getName(), getter);
                        
                        MethodHandle setter = lookup.unreflectSetter(field);
                        fieldSetters.put(field.getName(), setter);
                    }
                }
            } catch (IllegalAccessException | SecurityException e) {
                // If we can't create MethodHandles, continue with empty maps
                // (entity materialization will fall back to reflection)
            }

            MethodHandle prePersistHandle = findLifecycleHandle(entityClass,
                "io.memris.core.PrePersist"
            );
            MethodHandle postLoadHandle = findLifecycleHandle(entityClass,
                "io.memris.core.PostLoad"
            );
            MethodHandle preUpdateHandle = findLifecycleHandle(entityClass,
                "io.memris.core.PreUpdate"
            );
            
            return new EntityMetadata<>(
                entityClass,
                constructor,
                idColumnName,
                fields,
                foreignKeyColumns,
                Map.copyOf(converters),
                Map.of(),
                List.copyOf(indexDefinitions),
                prePersistHandle, postLoadHandle, preUpdateHandle,
                List.copyOf(auditFields),
                fieldGetters, fieldSetters,
                entityClass.isRecord()
            );
        } catch (NoSuchMethodException | SecurityException e) {
            throw new RuntimeException("Failed to extract metadata for " + entityClass.getName(), e);
        }
    }
    
    /**
     * Create a FieldMapping for a relationship field.
     */
    private static FieldMapping createRelationshipMapping(
        Field field,
        ManyToOne manyToOne,
        JoinColumn joinColumn,
        int colPos,
        Set<String> foreignKeyColumns,
        EntityMetadata.FieldMapping.RelationshipType relationshipType
    ) {
        // Determine target entity class
        Class<?> targetEntity = null;
        if (manyToOne != null && manyToOne.targetEntity() != void.class) {
            targetEntity = manyToOne.targetEntity();
        }
        if (targetEntity == null || targetEntity == void.class) {
            // Use field type as target entity
            targetEntity = field.getType();
        }
        
        // Determine foreign key column name
        String fkColumnName;
        if (joinColumn != null && !joinColumn.name().isEmpty()) {
            fkColumnName = joinColumn.name();
        } else {
            // Default: {fieldName}_{referencedColumnName}
            String refColumn = referencedColumnName(joinColumn);
            fkColumnName = field.getName() + "_" + refColumn;
        }

        String referencedColumnName = referencedColumnName(joinColumn);
        
        // Add to foreign key columns set
        foreignKeyColumns.add(fkColumnName);
        
        // Store relationship as target ID type (foreign key)
        Class<?> idFieldType = resolveIdFieldType(targetEntity);
        Class<?> storageType = resolveStorageType(idFieldType);
        Byte typeCode = TypeCodes.forClassOrDefault(storageType, TypeCodes.TYPE_LONG);
        
        return new FieldMapping(
            field.getName(),           // property name
            fkColumnName,              // column name (the FK column)
            field.getType(),           // Java type (the entity class)
            storageType,               // Storage type (the ID type)
            colPos,
            typeCode,
            true,                      // isRelationship
            relationshipType,
            targetEntity,
            null,                      // joinTable (not used for @ManyToOne)
            referencedColumnName,
            null,
            false,                     // isCollection
            false                      // isEmbedded
        );
    }

    private static FieldMapping createOneToManyMapping(
        Class<?> sourceEntity,
        Field field,
        OneToMany oneToMany
    ) {
        Class<?> targetEntity = resolveTargetEntity(field,
            oneToMany != null ? oneToMany.targetEntity() : void.class
        );

        var mappedBy = oneToMany != null ? oneToMany.mappedBy() : null;
        if (mappedBy == null || mappedBy.isBlank()) {
            throw new IllegalArgumentException("@OneToMany requires mappedBy: " + field.getDeclaringClass().getName() + "#" + field.getName());
        }

        Field mappedField;
        try {
            mappedField = targetEntity.getDeclaredField(mappedBy);
        } catch (NoSuchFieldException e) {
            throw new IllegalArgumentException("mappedBy field not found: " + targetEntity.getName() + "#" + mappedBy, e);
        }

        JoinColumn joinColumn = mappedField.getAnnotation(JoinColumn.class);
        String fkColumnName;
        if (joinColumn != null && !joinColumn.name().isEmpty()) {
            fkColumnName = joinColumn.name();
        } else {
            String refColumn = referencedColumnName(joinColumn);
            fkColumnName = mappedField.getName() + "_" + refColumn;
        }

        String referencedColumnName = referencedColumnName(joinColumn);

        Class<?> idFieldType = resolveIdFieldType(sourceEntity);
        Class<?> storageType = resolveStorageType(idFieldType);
        Byte typeCode = TypeCodes.forClassOrDefault(storageType, TypeCodes.TYPE_LONG);

        return new FieldMapping(
            field.getName(),
            fkColumnName,
            field.getType(),
            storageType,
            -1,
            typeCode,
            true,
            RelationshipType.ONE_TO_MANY,
            targetEntity,
            null,
            referencedColumnName,
            mappedBy,
            true,
            false
        );
    }

    private static FieldMapping createManyToManyMapping(
        Class<?> sourceEntity,
        Field field,
        ManyToMany manyToMany,
        JoinTable joinTable
    ) {
        Class<?> targetEntity = resolveTargetEntity(field,
            manyToMany != null ? manyToMany.targetEntity() : void.class
        );

        String mappedBy = manyToMany != null ? manyToMany.mappedBy() : null;
        boolean owningSide = mappedBy == null || mappedBy.isBlank();

        String joinTableName = null;
        String joinColumnName = null;
        String inverseJoinColumnName = null;

        if (owningSide) {
            if (joinTable != null && !joinTable.name().isEmpty()) {
                joinTableName = joinTable.name();
                joinColumnName = joinTable.joinColumn();
                inverseJoinColumnName = joinTable.inverseJoinColumn();
            }
            if (joinTableName == null || joinTableName.isBlank()) {
                joinTableName = sourceEntity.getSimpleName() + "_" + targetEntity.getSimpleName();
            }
            // Provide default column names if not specified
            if (joinColumnName == null || joinColumnName.isBlank()) {
                joinColumnName = sourceEntity.getSimpleName().toLowerCase() + "_id";
            }
            if (inverseJoinColumnName == null || inverseJoinColumnName.isBlank()) {
                inverseJoinColumnName = targetEntity.getSimpleName().toLowerCase() + "_id";
            }
        }

        Class<?> idFieldType = resolveIdFieldType(sourceEntity);
        Class<?> storageType = resolveStorageType(idFieldType);
        Byte typeCode = TypeCodes.forClassOrDefault(storageType, TypeCodes.TYPE_LONG);

        return new FieldMapping(
            field.getName(),
            joinColumnName,
            field.getType(),
            storageType,
            -1,
            typeCode,
            true,
            RelationshipType.MANY_TO_MANY,
            targetEntity,
            joinTableName,
            inverseJoinColumnName,
            owningSide ? null : mappedBy,
            true,
            false
        );
    }

    private static Class<?> resolveTargetEntity(Field field, Class<?> targetHint) {
        if (targetHint != null && targetHint != void.class) {
            return targetHint;
        }
        Class<?> elementType = resolveCollectionElementType(field);
        if (elementType != null) {
            return elementType;
        }
        return field.getType();
    }

    private static Class<?> resolveCollectionElementType(Field field) {
        Class<?> fieldType = field.getType();
        if (!Collection.class.isAssignableFrom(fieldType)) {
            return null;
        }
        Type genericType = field.getGenericType();
        if (genericType instanceof ParameterizedType pt) {
            Type[] args = pt.getActualTypeArguments();
            if (args.length > 0 && args[0] instanceof Class<?>) {
                return (Class<?>) args[0];
            }
        }
        return null;
    }

    private static EntityMetadata.AuditFieldType auditTypeForField(Field field) {
        if (hasAnnotationByName(field, "io.memris.core.CreatedDate")) {
            return EntityMetadata.AuditFieldType.CREATED_DATE;
        }
        if (hasAnnotationByName(field, "io.memris.core.LastModifiedDate")) {
            return EntityMetadata.AuditFieldType.LAST_MODIFIED_DATE;
        }
        if (hasAnnotationByName(field, "io.memris.core.CreatedBy")) {
            return EntityMetadata.AuditFieldType.CREATED_BY;
        }
        if (hasAnnotationByName(field, "io.memris.core.LastModifiedBy")) {
            return EntityMetadata.AuditFieldType.LAST_MODIFIED_BY;
        }
        return null;
    }

    private static MethodHandle findLifecycleHandle(Class<?> entityClass, String... annotationNames) {
        for (Method method : entityClass.getDeclaredMethods()) {
            if (!hasAnnotationByName(method, annotationNames)) {
                continue;
            }
            if (method.getParameterCount() != 0) {
                throw new IllegalArgumentException("Lifecycle method must have no parameters: " + entityClass.getName() + "#" + method.getName());
            }
            try {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                MethodHandles.Lookup privateLookup = MethodHandles.privateLookupIn(entityClass, lookup);
                return privateLookup.unreflect(method);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to access lifecycle method: " + entityClass.getName() + "#" + method.getName(), e);
            }
        }
        return null;
    }

    private static boolean hasAnnotationByName(AnnotatedElement element, String... annotationNames) {
        for (var annotation : element.getAnnotations()) {
            var name = annotation.annotationType().getName();
            for (String expected : annotationNames) {
                if (expected.equals(name)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static String referencedColumnName(JoinColumn joinColumn) {
        if (joinColumn != null && !joinColumn.referencedColumnName().isEmpty()) {
            return joinColumn.referencedColumnName();
        }
        return "id";
    }

    private static void handleFieldIndexAnnotations(Field field, int columnPosition, byte typeCode,
            List<IndexDefinition> indexDefinitions) {
        var index = field.getAnnotation(Index.class);
        if (index == null) {
            return;
        }
        var name = index.name() != null && !index.name().isBlank() ? index.name() : field.getName();
        indexDefinitions.add(new IndexDefinition(
                name,
                new String[] { field.getName() },
                new int[] { columnPosition },
                new byte[] { typeCode },
                index.type(),
                "memris"));
    }

    private static void handleClassLevelIndexAnnotations(Class<?> entityClass,
            List<FieldMapping> fields,
            List<IndexDefinition> indexDefinitions) {
        var declared = entityClass.getAnnotationsByType(Index.class);
        if (declared == null || declared.length == 0) {
            return;
        }
        var fieldByName = new HashMap<String, FieldMapping>(fields.size());
        for (var field : fields) {
            fieldByName.put(field.name(), field);
        }

        for (var index : declared) {
            var declaredFields = index.fields();
            if (declaredFields == null || declaredFields.length == 0) {
                continue;
            }
            if (declaredFields.length > 1
                    && (index.type() == Index.IndexType.PREFIX || index.type() == Index.IndexType.SUFFIX)) {
                throw new IllegalArgumentException("Composite indexes support HASH/BTREE only: " + entityClass.getName());
            }

            var unique = new HashSet<String>();
            var resolved = new ArrayList<FieldMapping>(declaredFields.length);
            for (var fieldName : declaredFields) {
                if (!unique.add(fieldName)) {
                    throw new IllegalArgumentException("Duplicate field in composite index: " + fieldName);
                }
                var mapping = fieldByName.get(fieldName);
                if (mapping == null) {
                    throw new IllegalArgumentException("Unknown index field '" + fieldName + "' in "
                            + entityClass.getName());
                }
                if (mapping.columnPosition() < 0) {
                    throw new IllegalArgumentException("Index field is not persisted: " + fieldName);
                }
                resolved.add(mapping);
            }

            var columnPositions = new int[resolved.size()];
            var typeCodes = new byte[resolved.size()];
            for (var i = 0; i < resolved.size(); i++) {
                var mapping = resolved.get(i);
                columnPositions[i] = mapping.columnPosition();
                typeCodes[i] = mapping.typeCode();
            }
            var name = index.name() != null && !index.name().isBlank()
                    ? index.name()
                    : String.join("_", declaredFields);
            indexDefinitions.add(new IndexDefinition(
                    name,
                    declaredFields,
                    columnPositions,
                    typeCodes,
                    index.type(),
                    "memris"));
        }
    }


    private static Class<?> resolveIdFieldType(Class<?> entityClass) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equals("id")
                    || field.isAnnotationPresent(GeneratedValue.class)) {
                return field.getType();
            }
        }
        return Long.class;
    }

    private static Class<?> resolveStorageType(Class<?> fieldType) {
        TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(fieldType);
        if (converter != null) {
            return converter.storageType();
        }
        return fieldType;
    }

    private static byte resolveTypeCode(Class<?> javaType, Class<?> storageType) {
        try {
            return TypeCodes.forClass(javaType);
        } catch (IllegalArgumentException ex) {
            return TypeCodes.forClassOrDefault(storageType, TypeCodes.TYPE_LONG);
        }
    }
}

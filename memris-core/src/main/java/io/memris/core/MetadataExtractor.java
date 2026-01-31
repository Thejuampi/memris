package io.memris.core;

import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.EntityMetadata.FieldMapping.RelationshipType;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import jakarta.persistence.Id;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.*;

/**
 * Extracts metadata from entity classes for repository generation.
 * All reflection work happens once during metadata extraction.
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
                if (field.getName().equals("id") || 
                    field.isAnnotationPresent(Id.class) ||
                    field.isAnnotationPresent(GeneratedValue.class)) {
                    idColumnName = field.getName();
                    break;
                }
            }
            
            // Build field mappings with relationship support
            List<FieldMapping> fields = new ArrayList<>();
            Map<String, TypeConverter<?, ?>> converters = new HashMap<>();
            Set<String> foreignKeyColumns = new HashSet<>();
            int colPos = 0;
            
            for (var field : entityClass.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                
                // Check for relationship annotations
                ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
                OneToOne oneToOne = field.getAnnotation(OneToOne.class);
                JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
                jakarta.persistence.ManyToOne jpaManyToOne = field.getAnnotation(jakarta.persistence.ManyToOne.class);
                jakarta.persistence.OneToOne jpaOneToOne = field.getAnnotation(jakarta.persistence.OneToOne.class);
                jakarta.persistence.JoinColumn jpaJoinColumn = field.getAnnotation(jakarta.persistence.JoinColumn.class);

                if (manyToOne != null || jpaManyToOne != null) {
                    FieldMapping relationshipMapping = createRelationshipMapping(
                        field,
                        manyToOne,
                        joinColumn,
                        jpaManyToOne,
                        jpaJoinColumn,
                        colPos,
                        foreignKeyColumns,
                        RelationshipType.MANY_TO_ONE
                    );
                    fields.add(relationshipMapping);
                    colPos++;
                } else if (oneToOne != null || jpaOneToOne != null) {
                    FieldMapping relationshipMapping = createRelationshipMapping(
                        field,
                        null,
                        joinColumn,
                        null,
                        jpaJoinColumn,
                        colPos,
                        foreignKeyColumns,
                        RelationshipType.ONE_TO_ONE
                    );
                    fields.add(relationshipMapping);
                    colPos++;
                } else {
                    // Regular field
                    TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(field.getType());
                    Class<?> storageType = converter != null ? converter.getStorageType() : field.getType();
                    byte typeCode = resolveTypeCode(field.getType(), storageType);
                    if (converter != null) {
                        converters.put(field.getName(), converter);
                    }
                    fields.add(new FieldMapping(
                        field.getName(),
                        field.getName(),
                        field.getType(),
                        storageType,
                        colPos++,
                        typeCode
                    ));
                }
            }
            
            // Build MethodHandles for field access (for public fields)
            Map<String, MethodHandle> fieldGetters = new HashMap<>();
            Map<String, MethodHandle> fieldSetters = new HashMap<>();
            
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
            } catch (Exception e) {
                // If we can't create MethodHandles, continue with empty maps
                // (entity materialization will fall back to reflection)
            }
            
            return new EntityMetadata<>(
                entityClass,
                constructor,
                idColumnName,
                fields,
                foreignKeyColumns,
                Map.copyOf(converters),
                null, null, null,
                fieldGetters, fieldSetters,
                entityClass.isRecord()
            );
        } catch (Exception e) {
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
        jakarta.persistence.ManyToOne jpaManyToOne,
        jakarta.persistence.JoinColumn jpaJoinColumn,
        int colPos,
        Set<String> foreignKeyColumns,
        EntityMetadata.FieldMapping.RelationshipType relationshipType
    ) {
        // Determine target entity class
        Class<?> targetEntity = null;
        if (manyToOne != null && manyToOne.targetEntity() != void.class) {
            targetEntity = manyToOne.targetEntity();
        } else if (jpaManyToOne != null && jpaManyToOne.targetEntity() != void.class) {
            targetEntity = jpaManyToOne.targetEntity();
        }
        if (targetEntity == null || targetEntity == void.class) {
            // Use field type as target entity
            targetEntity = field.getType();
        }
        
        // Determine foreign key column name
        String fkColumnName;
        if (joinColumn != null && !joinColumn.name().isEmpty()) {
            fkColumnName = joinColumn.name();
        } else if (jpaJoinColumn != null && !jpaJoinColumn.name().isEmpty()) {
            fkColumnName = jpaJoinColumn.name();
        } else {
            // Default: {fieldName}_{referencedColumnName}
            String refColumn = referencedColumnName(joinColumn, jpaJoinColumn);
            fkColumnName = field.getName() + "_" + refColumn;
        }

        String referencedColumnName = referencedColumnName(joinColumn, jpaJoinColumn);
        
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
            false,                     // isCollection
            false                      // isEmbedded
        );
    }

    private static String referencedColumnName(JoinColumn joinColumn, jakarta.persistence.JoinColumn jpaJoinColumn) {
        if (joinColumn != null && !joinColumn.referencedColumnName().isEmpty()) {
            return joinColumn.referencedColumnName();
        }
        if (jpaJoinColumn != null && !jpaJoinColumn.referencedColumnName().isEmpty()) {
            return jpaJoinColumn.referencedColumnName();
        }
        return "id";
    }

    private static Class<?> resolveIdFieldType(Class<?> entityClass) {
        for (Field field : entityClass.getDeclaredFields()) {
            if (field.getName().equals("id") || field.isAnnotationPresent(Id.class) || field.isAnnotationPresent(GeneratedValue.class)) {
                return field.getType();
            }
        }
        return Long.class;
    }

    private static Class<?> resolveStorageType(Class<?> fieldType) {
        TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance().getConverter(fieldType);
        if (converter != null) {
            return converter.getStorageType();
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

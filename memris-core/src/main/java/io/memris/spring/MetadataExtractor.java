package io.memris.spring;

import io.memris.spring.EntityMetadata.FieldMapping;
import io.memris.spring.EntityMetadata.FieldMapping.RelationshipType;
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
            Set<String> foreignKeyColumns = new HashSet<>();
            int colPos = 0;
            
            for (var field : entityClass.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                
                // Check for relationship annotations
                ManyToOne manyToOne = field.getAnnotation(ManyToOne.class);
                JoinColumn joinColumn = field.getAnnotation(JoinColumn.class);
                
                if (manyToOne != null) {
                    // This is a relationship field
                    FieldMapping relationshipMapping = createRelationshipMapping(
                        field, manyToOne, joinColumn, colPos, foreignKeyColumns
                    );
                    fields.add(relationshipMapping);
                    colPos++;
                } else {
                    // Regular field
                    Byte typeCode = TypeCodes.forClassOrDefault(field.getType(), TypeCodes.TYPE_LONG);
                    fields.add(new FieldMapping(
                        field.getName(),
                        field.getName(),
                        field.getType(),
                        field.getType(),
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
                Map.of(),
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
        int colPos,
        Set<String> foreignKeyColumns
    ) {
        // Determine target entity class
        Class<?> targetEntity = manyToOne.targetEntity();
        if (targetEntity == void.class) {
            // Use field type as target entity
            targetEntity = field.getType();
        }
        
        // Determine foreign key column name
        String fkColumnName;
        if (joinColumn != null && !joinColumn.name().isEmpty()) {
            fkColumnName = joinColumn.name();
        } else {
            // Default: {fieldName}_{referencedColumnName}
            String refColumn = (joinColumn != null) ? joinColumn.referencedColumnName() : "id";
            fkColumnName = field.getName() + "_" + refColumn;
        }
        
        // Add to foreign key columns set
        foreignKeyColumns.add(fkColumnName);
        
        // For now, store relationship as Long (foreign key ID)
        // In the future, could support storing the actual entity reference
        Byte typeCode = TypeCodes.TYPE_LONG;
        
        return new FieldMapping(
            field.getName(),           // property name
            fkColumnName,              // column name (the FK column)
            field.getType(),           // Java type (the entity class)
            Long.class,                // Storage type (the ID type)
            colPos,
            typeCode,
            true,                      // isRelationship
            RelationshipType.MANY_TO_ONE,
            targetEntity,
            null,                      // joinTable (not used for @ManyToOne)
            false,                     // isCollection
            false                      // isEmbedded
        );
    }
}

package io.memris.storage.heap;

/**
 * Metadata for a single field in an entity.
 */
public record FieldMetadata(
        String name,
        byte typeCode,
        boolean isId,
        boolean isPrimaryKey,
        boolean primitiveNonNull
) {
    public FieldMetadata(String name, byte typeCode, boolean isId, boolean isPrimaryKey) {
        this(name, typeCode, isId, isPrimaryKey, false);
    }

    /**
     * Get the type code as a byte.
     * @return the type code
     */
    public byte type() {
        return typeCode;
    }
}

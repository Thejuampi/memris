package io.memris.core;

/**
 * ID generation strategy declarations.
 * The default engine implements AUTO and IDENTITY for Long and Integer IDs.
 */
public enum GenerationType {
    /**
     * Numeric ID generation using the repository counter.
     */
    AUTO,

    /**
     * Numeric ID generation using the repository counter, as with AUTO.
     */
    IDENTITY,

    /**
     * Reserved UUID generation; rejected by the default metadata provider.
     */
    UUID,

    /**
     * Reserved custom generation; rejected by the default metadata provider.
     */
    CUSTOM
}

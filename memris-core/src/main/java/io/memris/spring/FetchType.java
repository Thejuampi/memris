package io.memris.spring;

/**
 * Defines fetch type for entity relationships.
 * <p>
 * <b>Note:</b> Memris is an in-memory storage engine, so all relationships
 * are always loaded eagerly. There is no lazy loading support.
 * 
 * @see ManyToOne
 * @deprecated This enum is kept for API compatibility but only EAGER fetching is supported.
 */
@Deprecated
public enum FetchType {
    
    /**
     * Related entity is loaded immediately when the parent entity is loaded.
     * This is the only supported mode in Memris.
     */
    EAGER
}

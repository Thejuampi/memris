package io.memris.util;

/**
 * Utility class for naming convention conversions.
 */
public final class NamingUtils {
    
    private NamingUtils() {}
    
    /**
     * Converts camelCase to snake_case.
     * <p>
     * Examples:
     * <ul>
     *   <li>userId → user_id</li>
     *   <li>stockQuantity → stock_quantity</li>
     *   <li>emailVerified → email_verified</li>
     *   <li>URLPattern → url_pattern (handles consecutive capitals)</li>
     *   <li>id → id (single word unchanged)</li>
     * </ul>
     * 
     * @param camelCase the camelCase string
     * @return the snake_case equivalent
     */
    public static String toSnakeCase(String camelCase) {
        if (camelCase == null || camelCase.isEmpty()) {
            return camelCase;
        }
        
        StringBuilder result = new StringBuilder();
        char prevChar = camelCase.charAt(0);
        result.append(Character.toLowerCase(prevChar));
        
        for (int i = 1; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            
            // Insert underscore before uppercase letters
            if (Character.isUpperCase(c)) {
                // Don't insert if previous was also uppercase (handles URL, HTTP, etc.)
                if (!Character.isUpperCase(prevChar)) {
                    result.append('_');
                }
                result.append(Character.toLowerCase(c));
            } else {
                result.append(c);
            }
            
            prevChar = c;
        }
        
        return result.toString();
    }
    
    /**
     * Converts snake_case to camelCase (lower camelCase).
     * <p>
     * Examples:
     * <ul>
     *   <li>user_id → userId</li>
     *   <li>stock_quantity → stockQuantity</li>
     *   <li>email_verified → emailVerified</li>
     *   <li>id → id</li>
     * </ul>
     * 
     * @param snakeCase the snake_case string
     * @return the camelCase equivalent
     */
    public static String toCamelCase(String snakeCase) {
        if (snakeCase == null || snakeCase.isEmpty()) {
            return snakeCase;
        }
        
        StringBuilder result = new StringBuilder();
        boolean capitalizeNext = false;
        
        for (int i = 0; i < snakeCase.length(); i++) {
            char c = snakeCase.charAt(i);
            
            if (c == '_') {
                capitalizeNext = true;
            } else if (capitalizeNext) {
                result.append(Character.toUpperCase(c));
                capitalizeNext = false;
            } else {
                result.append(c);
            }
        }
        
        return result.toString();
    }
}

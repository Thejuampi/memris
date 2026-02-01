package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for PageColumnString scan operations.
 */
class PageColumnStringScanTest {

    private static final int PAGE_SIZE = 64;

    @Test
    @DisplayName("scanEquals should find matching strings")
    void scanEqualsFindsMatchingStrings() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "apple");
        column.set(1, "banana");
        column.set(2, "apple");
        column.set(3, "cherry");
        column.publish(4);
        
        int[] matches = column.scanEquals("apple", 4);
        
        assertArrayEquals(new int[]{0, 2}, matches);
    }
    
    @Test
    @DisplayName("scanEquals should handle null target")
    void scanEqualsNullTarget() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "apple");
        column.setNull(1);
        column.set(2, "banana");
        column.setNull(3);
        column.publish(4);
        
        int[] matches = column.scanEquals(null, 4);
        
        assertArrayEquals(new int[]{1, 3}, matches);
    }
    
    @Test
    @DisplayName("scanEqualsIgnoreCase should find strings ignoring case")
    void scanEqualsIgnoreCase() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "Apple");
        column.set(1, "APPLE");
        column.set(2, "banana");
        column.set(3, "aPPle");
        column.publish(4);
        
        int[] matches = column.scanEqualsIgnoreCase("apple", 4);
        
        assertArrayEquals(new int[]{0, 1, 3}, matches);
    }
    
    @Test
    @DisplayName("scanEqualsIgnoreCase should handle null target")
    void scanEqualsIgnoreCaseNullTarget() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "apple");
        column.setNull(1);
        column.set(2, "banana");
        column.publish(3);
        
        int[] matches = column.scanEqualsIgnoreCase(null, 3);
        
        assertArrayEquals(new int[]{1}, matches);
    }
    
    @Test
    @DisplayName("scanIn should find strings in target set")
    void scanIn() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "red");
        column.set(1, "green");
        column.set(2, "blue");
        column.set(3, "yellow");
        column.publish(4);
        
        int[] matches = column.scanIn(new String[]{"green", "yellow"}, 4);
        
        assertArrayEquals(new int[]{1, 3}, matches);
    }
    
    @Test
    @DisplayName("scanIn should skip null in target set")
    void scanInWithNullInTargets() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "red");
        column.setNull(1);
        column.set(2, "blue");
        column.publish(3);
        
        // scanIn skips null values in the target array
        int[] matches = column.scanIn(new String[]{"red", null}, 3);
        
        assertArrayEquals(new int[]{0}, matches);
    }
    
    @Test
    @DisplayName("scanIn should return empty for empty targets")
    void scanInEmptyTargets() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "red");
        column.set(1, "green");
        column.publish(2);
        
        int[] matches = column.scanIn(new String[0], 2);
        
        assertArrayEquals(new int[0], matches);
    }
    
    @Test
    @DisplayName("scanIn should return empty for null targets")
    void scanInNullTargets() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "red");
        column.set(1, "green");
        column.publish(2);
        
        int[] matches = column.scanIn(null, 2);
        
        assertArrayEquals(new int[0], matches);
    }
    
    @Test
    @DisplayName("scan methods should respect published count")
    void scanRespectsPublishedCount() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "apple");
        column.set(1, "apple");
        column.set(2, "apple"); // Not published
        column.publish(2);
        
        int[] matches = column.scanEquals("apple", 4);
        
        assertArrayEquals(new int[]{0, 1}, matches);
    }
    
    @Test
    @DisplayName("empty string handling")
    void emptyStringHandling() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "");
        column.set(1, "a");
        column.set(2, "");
        column.publish(3);
        
        int[] matches = column.scanEquals("", 3);
        
        assertArrayEquals(new int[]{0, 2}, matches);
    }
    
    @Test
    @DisplayName("unicode string handling")
    void unicodeStringHandling() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "Hello");
        column.set(1, "こんにちは");
        column.set(2, "🎉 Emoji");
        column.publish(3);
        
        assertArrayEquals(new int[]{1}, column.scanEquals("こんにちは", 3));
        assertArrayEquals(new int[]{2}, column.scanEquals("🎉 Emoji", 3));
    }
    
    @Test
    @DisplayName("no matches returns empty array")
    void noMatchesReturnsEmpty() {
        PageColumnString column = new PageColumnString(PAGE_SIZE);
        
        column.set(0, "apple");
        column.set(1, "banana");
        column.publish(2);
        
        assertArrayEquals(new int[0], column.scanEquals("orange", 2));
        assertArrayEquals(new int[0], column.scanIn(new String[]{"grape", "pear"}, 2));
    }
}

package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for PageColumnLong scan operations.
 */
class PageColumnLongScanTest {

    private static final int PAGE_SIZE = 64;

    @Test
    @DisplayName("scanEquals should find matching values")
    void scanEqualsFindsMatchingValues() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 100L);
        column.set(1, 200L);
        column.set(2, 100L);
        column.set(3, 300L);
        column.publish(4);
        
        int[] matches = column.scanEquals(100L, 4);
        
        assertArrayEquals(new int[]{0, 2}, matches);
    }
    
    @Test
    @DisplayName("scanGreaterThan should find values strictly greater than target")
    void scanGreaterThan() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.set(2, 30L);
        column.set(3, 40L);
        column.publish(4);
        
        int[] matches = column.scanGreaterThan(20L, 4);
        
        assertArrayEquals(new int[]{2, 3}, matches);
    }
    
    @Test
    @DisplayName("scanGreaterThanOrEqual should find values greater than or equal to target")
    void scanGreaterThanOrEqual() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.set(2, 30L);
        column.publish(3);
        
        int[] matches = column.scanGreaterThanOrEqual(20L, 3);
        
        assertArrayEquals(new int[]{1, 2}, matches);
    }
    
    @Test
    @DisplayName("scanLessThan should find values strictly less than target")
    void scanLessThan() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.set(2, 30L);
        column.publish(3);
        
        int[] matches = column.scanLessThan(30L, 3);
        
        assertArrayEquals(new int[]{0, 1}, matches);
    }
    
    @Test
    @DisplayName("scanLessThanOrEqual should find values less than or equal to target")
    void scanLessThanOrEqual() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.set(2, 30L);
        column.publish(3);
        
        int[] matches = column.scanLessThanOrEqual(20L, 3);
        
        assertArrayEquals(new int[]{0, 1}, matches);
    }
    
    @Test
    @DisplayName("scanBetween should find values within inclusive range")
    void scanBetween() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 5L);
        column.set(1, 10L);
        column.set(2, 15L);
        column.set(3, 20L);
        column.set(4, 25L);
        column.publish(5);
        
        int[] matches = column.scanBetween(10L, 20L, 5);
        
        assertArrayEquals(new int[]{1, 2, 3}, matches);
    }
    
    @Test
    @DisplayName("scanIn should find values in target set")
    void scanIn() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.set(2, 30L);
        column.set(3, 40L);
        column.set(4, 50L);
        column.publish(5);
        
        int[] matches = column.scanIn(new long[]{20L, 40L, 60L}, 5);
        
        assertArrayEquals(new int[]{1, 3}, matches);
    }
    
    @Test
    @DisplayName("scanIn should return empty array for empty targets")
    void scanInEmptyTargets() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.publish(2);
        
        int[] matches = column.scanIn(new long[0], 2);
        
        assertArrayEquals(new int[0], matches);
    }
    
    @Test
    @DisplayName("scanIn should return empty array for null targets")
    void scanInNullTargets() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.publish(2);
        
        int[] matches = column.scanIn(null, 2);
        
        assertArrayEquals(new int[0], matches);
    }
    
    @Test
    @DisplayName("boundary conditions with extreme values")
    void extremeValueBoundaries() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, Long.MAX_VALUE);
        column.set(1, 0L);
        column.set(2, Long.MIN_VALUE);
        column.publish(3);
        
        assertArrayEquals(new int[]{0}, column.scanEquals(Long.MAX_VALUE, 3));
        assertArrayEquals(new int[]{2}, column.scanEquals(Long.MIN_VALUE, 3));
        assertArrayEquals(new int[]{0}, column.scanGreaterThan(0L, 3));
        assertArrayEquals(new int[]{2}, column.scanLessThan(0L, 3));
    }
    
    @Test
    @DisplayName("scan methods should return empty array when no matches")
    void scanNoMatchesReturnsEmpty() {
        PageColumnLong column = new PageColumnLong(PAGE_SIZE);
        
        column.set(0, 10L);
        column.set(1, 20L);
        column.publish(2);
        
        assertArrayEquals(new int[0], column.scanEquals(99L, 2));
        assertArrayEquals(new int[0], column.scanGreaterThan(50L, 2));
        assertArrayEquals(new int[0], column.scanLessThan(5L, 2));
        assertArrayEquals(new int[0], column.scanBetween(100L, 200L, 2));
        assertArrayEquals(new int[0], column.scanIn(new long[]{99L, 999L}, 2));
    }
}

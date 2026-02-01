package io.memris.storage.heap;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive tests for PageColumnInt scan operations.
 * Tests all scan methods including edge cases and boundary conditions.
 */
class PageColumnIntScanTest {

    private static final int PAGE_SIZE = 64;

    @Test
    @DisplayName("scanEquals should find matching values")
    void scanEqualsFindsMatchingValues() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 100);
        column.set(1, 200);
        column.set(2, 100);
        column.set(3, 300);
        column.publish(4);
        
        int[] matches = column.scanEquals(100, 4);
        
        assertArrayEquals(new int[]{0, 2}, matches);
    }
    
    @Test
    @DisplayName("scanEquals should respect published count limit")
    void scanEqualsRespectsPublishedCount() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 100);
        column.set(1, 100);
        column.set(2, 100); // Not published
        column.publish(2);
        
        int[] matches = column.scanEquals(100, 4);
        
        assertArrayEquals(new int[]{0, 1}, matches);
    }
    
    @Test
    @DisplayName("scanGreaterThan should find values strictly greater than target")
    void scanGreaterThan() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.set(2, 30);
        column.set(3, 40);
        column.publish(4);
        
        int[] matches = column.scanGreaterThan(20, 4);
        
        assertArrayEquals(new int[]{2, 3}, matches);
    }
    
    @Test
    @DisplayName("scanGreaterThanOrEqual should find values greater than or equal to target")
    void scanGreaterThanOrEqual() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.set(2, 30);
        column.set(3, 40);
        column.publish(4);
        
        int[] matches = column.scanGreaterThanOrEqual(20, 4);
        
        assertArrayEquals(new int[]{1, 2, 3}, matches);
    }
    
    @Test
    @DisplayName("scanLessThan should find values strictly less than target")
    void scanLessThan() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.set(2, 30);
        column.set(3, 40);
        column.publish(4);
        
        int[] matches = column.scanLessThan(30, 4);
        
        assertArrayEquals(new int[]{0, 1}, matches);
    }
    
    @Test
    @DisplayName("scanLessThanOrEqual should find values less than or equal to target")
    void scanLessThanOrEqual() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.set(2, 30);
        column.set(3, 40);
        column.publish(4);
        
        int[] matches = column.scanLessThanOrEqual(30, 4);
        
        assertArrayEquals(new int[]{0, 1, 2}, matches);
    }
    
    @Test
    @DisplayName("scanBetween should find values within inclusive range")
    void scanBetween() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 5);
        column.set(1, 10);
        column.set(2, 15);
        column.set(3, 20);
        column.set(4, 25);
        column.publish(5);
        
        int[] matches = column.scanBetween(10, 20, 5);
        
        assertArrayEquals(new int[]{1, 2, 3}, matches);
    }
    
    @Test
    @DisplayName("scanBetween should handle boundary values correctly")
    void scanBetweenBoundaryValues() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, Integer.MIN_VALUE);
        column.set(1, 0);
        column.set(2, Integer.MAX_VALUE);
        column.publish(3);
        
        int[] matches = column.scanBetween(Integer.MIN_VALUE, Integer.MAX_VALUE, 3);
        
        assertArrayEquals(new int[]{0, 1, 2}, matches);
    }
    
    @Test
    @DisplayName("scanIn should find values in target set")
    void scanIn() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.set(2, 30);
        column.set(3, 40);
        column.set(4, 50);
        column.publish(5);
        
        int[] matches = column.scanIn(new int[]{20, 40, 60}, 5);
        
        assertArrayEquals(new int[]{1, 3}, matches);
    }
    
    @Test
    @DisplayName("scanIn should return empty array for empty targets")
    void scanInEmptyTargets() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.publish(2);
        
        int[] matches = column.scanIn(new int[0], 2);
        
        assertArrayEquals(new int[0], matches);
    }
    
    @Test
    @DisplayName("scanIn should return empty array for null targets")
    void scanInNullTargets() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.publish(2);
        
        int[] matches = column.scanIn(null, 2);
        
        assertArrayEquals(new int[0], matches);
    }
    
    @Test
    @DisplayName("scan methods should skip null/unpublished values")
    void scanMethodsSkipNullValues() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 100);
        column.setNull(1); // Explicitly null
        column.set(2, 100);
        // Index 3 is simply not set (unpublished)
        column.publish(4);
        
        int[] matches = column.scanEquals(100, 4);
        
        assertArrayEquals(new int[]{0, 2}, matches);
    }
    
    @Test
    @DisplayName("scan methods should return empty array when no matches")
    void scanNoMatchesReturnsEmpty() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, 10);
        column.set(1, 20);
        column.publish(2);
        
        assertArrayEquals(new int[0], column.scanEquals(99, 2));
        assertArrayEquals(new int[0], column.scanGreaterThan(50, 2));
        assertArrayEquals(new int[0], column.scanLessThan(5, 2));
        assertArrayEquals(new int[0], column.scanBetween(100, 200, 2));
        assertArrayEquals(new int[0], column.scanIn(new int[]{99, 999}, 2));
    }
    
    @Test
    @DisplayName("boundary conditions with extreme values")
    void extremeValueBoundaries() {
        PageColumnInt column = new PageColumnInt(PAGE_SIZE);
        
        column.set(0, Integer.MAX_VALUE);
        column.set(1, 0);
        column.set(2, Integer.MIN_VALUE);
        column.publish(3);
        
        assertArrayEquals(new int[]{0}, column.scanEquals(Integer.MAX_VALUE, 3));
        assertArrayEquals(new int[]{2}, column.scanEquals(Integer.MIN_VALUE, 3));
        assertArrayEquals(new int[]{0}, column.scanGreaterThan(0, 3));
        assertArrayEquals(new int[]{2}, column.scanLessThan(0, 3));
    }
}

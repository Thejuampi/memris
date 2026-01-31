package io.memris.storage.heap;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lock-free stack implementation for free-list row ID reuse.
 * <p>
 * Uses Treiber stack algorithm with AtomicReference for lock-free
 * concurrent push/pop operations.
 */
final class LockFreeFreeList {
    
    private final AtomicReference<Node> head = new AtomicReference<>();
    private final AtomicInteger size = new AtomicInteger(0);
    
    /**
     * Push a row ID onto the stack.
     * Lock-free using CAS loop.
     * 
     * @param rowId the row ID to push
     */
    public void push(int rowId) {
        Node newHead = new Node(rowId);
        while (true) {
            Node currentHead = head.get();
            newHead.next = currentHead;
            if (head.compareAndSet(currentHead, newHead)) {
                size.incrementAndGet();
                return;
            }
            // CAS failed, retry
        }
    }
    
    /**
     * Pop a row ID from the stack.
     * Lock-free using CAS loop.
     * 
     * @return the row ID, or -1 if empty
     */
    public int pop() {
        while (true) {
            Node currentHead = head.get();
            if (currentHead == null) {
                return -1; // Empty
            }
            Node newHead = currentHead.next;
            if (head.compareAndSet(currentHead, newHead)) {
                size.decrementAndGet();
                return currentHead.rowId;
            }
            // CAS failed, retry
        }
    }
    
    /**
     * Check if the free-list is empty.
     * 
     * @return true if empty
     */
    public boolean isEmpty() {
        return head.get() == null;
    }
    
    /**
     * Get approximate size (may be stale).
     * 
     * @return approximate number of entries
     */
    public int size() {
        return size.get();
    }
    
    private static final class Node {
        final int rowId;
        Node next;
        
        Node(int rowId) {
            this.rowId = rowId;
        }
    }
}

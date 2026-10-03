package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * In-process fair (FIFO) wait queue for blocking acquire requests.
 *
 * <p>One queue per resource; waiters are served strictly in enqueue order and
 * only the head waiter may attempt a grant. Waiters park on their own monitor
 * (no busy spinning) and are woken when a lease is released, reaped, or when
 * the waiter ahead of them leaves the queue. Capacity is bounded per resource
 * and in total; overflowing requests are rejected with QUEUE_FULL instead of
 * piling up in memory.
 */
public final class LeaseWaitQueue {

    private static final Logger log = LoggerFactory.getLogger(LeaseWaitQueue.class);

    /** A single queued waiting request. */
    public static final class Waiter {
        private final String requestId;
        private final String resourceId;
        private final String holderId;
        private final Object monitor = new Object();
        private boolean signalled;

        private Waiter(String requestId, String resourceId, String holderId) {
            this.requestId = requestId;
            this.resourceId = resourceId;
            this.holderId = holderId;
        }

        public String requestId() { return requestId; }
        public String resourceId() { return resourceId; }
        public String holderId() { return holderId; }
    }

    private final int maxWaitersPerResource;
    private final int maxTotalWaiters;
    private final Map<String, Deque<Waiter>> queues = new HashMap<>();
    private int totalWaiters;

    public LeaseWaitQueue(int maxWaitersPerResource, int maxTotalWaiters) {
        this.maxWaitersPerResource = maxWaitersPerResource;
        this.maxTotalWaiters = maxTotalWaiters;
    }

    /**
     * Appends a waiter to the resource queue, or returns the already queued
     * waiter when the same requestId is re-submitted (idempotent join).
     *
     * @throws LeaseException with QUEUE_FULL when capacity is exhausted
     */
    public synchronized Waiter enqueueOrJoin(String resourceId, String requestId, String holderId) {
        Deque<Waiter> queue = queues.computeIfAbsent(resourceId, k -> new ArrayDeque<>());
        for (Waiter w : queue) {
            if (w.requestId.equals(requestId)) {
                log.info("wait-queue join existing requestId={} resource={} position={}",
                        requestId, resourceId, positionOf(queue, requestId));
                return w;
            }
        }
        if (queue.size() >= maxWaitersPerResource) {
            throw new LeaseException(LeaseErrorCode.QUEUE_FULL,
                    "wait queue for resource '" + resourceId + "' is full ("
                            + queue.size() + "/" + maxWaitersPerResource + ")");
        }
        if (totalWaiters >= maxTotalWaiters) {
            throw new LeaseException(LeaseErrorCode.QUEUE_FULL,
                    "global wait queue is full (" + totalWaiters + "/" + maxTotalWaiters + ")");
        }
        Waiter waiter = new Waiter(requestId, resourceId, holderId);
        queue.addLast(waiter);
        totalWaiters++;
        log.info("wait-queue enqueue requestId={} resource={} holder={} position={} queueSize={} total={}",
                requestId, resourceId, holderId, queue.size(), queue.size(), totalWaiters);
        return waiter;
    }

    /** True when the waiter is currently at the head of its resource queue. */
    public synchronized boolean isHead(Waiter waiter) {
        Deque<Waiter> queue = queues.get(waiter.resourceId);
        return queue != null && queue.peekFirst() == waiter;
    }

    /** Removes the waiter (grant, timeout or failure) and wakes the next head. */
    public void remove(Waiter waiter) {
        Waiter next;
        synchronized (this) {
            Deque<Waiter> queue = queues.get(waiter.resourceId);
            if (queue == null || !queue.remove(waiter)) {
                return;
            }
            totalWaiters--;
            if (queue.isEmpty()) {
                queues.remove(waiter.resourceId);
            }
            log.info("wait-queue dequeue requestId={} resource={} remaining={} total={}",
                    waiter.requestId, waiter.resourceId, queue.size(), totalWaiters);
            next = queue.isEmpty() ? null : queue.peekFirst();
        }
        if (next != null) {
            wake(next);
        }
    }

    /** Wakes the current head waiter of the resource, if any. */
    public void signal(String resourceId) {
        Waiter head;
        synchronized (this) {
            Deque<Waiter> queue = queues.get(resourceId);
            head = queue == null ? null : queue.peekFirst();
        }
        if (head != null) {
            wake(head);
        }
    }

    /**
     * Parks the waiter until signalled or the (real-time) slice elapses.
     * Short slices let waiters also notice clock-driven lease expiry.
     */
    public void awaitTurn(Waiter waiter, long millis) {
        synchronized (waiter.monitor) {
            if (waiter.signalled) {
                waiter.signalled = false;
                return;
            }
            try {
                waiter.monitor.wait(Math.max(1L, millis));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            waiter.signalled = false;
        }
    }

    /** Current queue length for a resource (diagnostics/tests). */
    public synchronized int size(String resourceId) {
        Deque<Waiter> queue = queues.get(resourceId);
        return queue == null ? 0 : queue.size();
    }

    /** Total queued waiters across all resources (diagnostics/tests). */
    public synchronized int totalSize() {
        return totalWaiters;
    }

    private static void wake(Waiter waiter) {
        synchronized (waiter.monitor) {
            waiter.signalled = true;
            waiter.monitor.notify();
        }
    }

    private static int positionOf(Deque<Waiter> queue, String requestId) {
        int pos = 1;
        for (Waiter w : queue) {
            if (w.requestId.equals(requestId)) {
                return pos;
            }
            pos++;
        }
        return -1;
    }
}

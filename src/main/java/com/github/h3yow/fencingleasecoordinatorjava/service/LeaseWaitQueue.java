package com.github.h3yow.fencingleasecoordinatorjava.service;

import com.github.h3yow.fencingleasecoordinatorjava.dto.LeaseResponse;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseErrorCode;
import com.github.h3yow.fencingleasecoordinatorjava.error.LeaseException;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * FIFO wait queue for a single resource. Only the head waiter may attempt to
 * acquire the lease, which guarantees grant order equals arrival order and that
 * at most one waiter wins. Waiters block on a condition (no busy spinning) and
 * are woken when the lease is released/reaped, when the current lease expires,
 * or when their own wait budget runs out. A waiter that leaves the queue
 * (granted, timed out, or failed) never attempts an acquire afterwards.
 */
class LeaseWaitQueue {

    /** Outcome of a head-of-queue acquire attempt. */
    record Attempt(LeaseResponse granted, long recheckAtMillis) {
        static Attempt granted(LeaseResponse r) {
            return new Attempt(r, Long.MAX_VALUE);
        }

        static Attempt stillHeld(long recheckAtMillis) {
            return new Attempt(null, recheckAtMillis);
        }
    }

    static final class Waiter {
        final String requestId;
        final String holderId;
        boolean done;
        LeaseResponse result;

        Waiter(String requestId, String holderId) {
            this.requestId = requestId;
            this.holderId = holderId;
        }
    }

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Condition changed = lock.newCondition();
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();
    private final int capacity;

    LeaseWaitQueue(int capacity) {
        this.capacity = capacity;
    }

    int size() {
        lock.lock();
        try {
            return waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Best-effort check used to give queued waiters priority over immediate
     * (non-waiting) acquires. Never blocks: while the head waiter is attempting
     * an acquire the lock is held, in which case we report "active".
     */
    boolean hasActiveWaiters() {
        if (!lock.tryLock()) {
            return true;
        }
        try {
            return !waiters.isEmpty();
        } finally {
            lock.unlock();
        }
    }

    /** Wake all waiters, e.g. after a release or after the reaper marks a lease EXPIRED. */
    void signalAll() {
        lock.lock();
        try {
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Join the queue and wait for a grant or the deadline. If a live waiter with
     * the same requestId is already queued (concurrent duplicate submission), this
     * call observes that waiter and returns its outcome instead of queueing twice.
     *
     * @param attempt       invoked only while this waiter is head of the queue
     * @param onQueueFull   builds the outcome when the queue is at capacity
     * @param onWaitTimeout builds the outcome when the wait budget is exhausted
     */
    LeaseResponse awaitTurn(Waiter waiter, long waitMillis, LongSupplier nowMillis,
                            Function<Waiter, Attempt> attempt,
                            Supplier<LeaseResponse> onQueueFull,
                            Supplier<LeaseResponse> onWaitTimeout) {
        lock.lock();
        try {
            Waiter existing = findByRequestId(waiter.requestId);
            if (existing != null) {
                LeaseResponse observed = observe(existing, nowMillis);
                if (observed != null) {
                    return observed;
                }
                // the observed waiter failed without a result; fall through and queue ourselves
            }
            if (waiters.size() >= capacity) {
                return onQueueFull.get();
            }
            long deadline = nowMillis.getAsLong() + waitMillis;
            waiters.addLast(waiter);
            try {
                while (true) {
                    long remaining = deadline - nowMillis.getAsLong();
                    if (remaining <= 0) {
                        return finish(waiter, onWaitTimeout.get());
                    }
                    if (waiters.peekFirst() == waiter) {
                        Attempt a = attempt.apply(waiter);
                        if (a.granted() != null) {
                            return finish(waiter, a.granted());
                        }
                        long untilRecheck = Math.max(1L, a.recheckAtMillis() - nowMillis.getAsLong());
                        awaitMillis(Math.min(remaining, untilRecheck));
                    } else {
                        awaitMillis(remaining);
                    }
                }
            } catch (RuntimeException e) {
                waiters.remove(waiter);
                waiter.done = true;
                throw e;
            } finally {
                // whether granted, timed out or failed: let the next waiter re-evaluate
                changed.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    private LeaseResponse finish(Waiter waiter, LeaseResponse outcome) {
        waiters.remove(waiter);
        waiter.result = outcome;
        waiter.done = true;
        return outcome;
    }

    /** Wait until the target waiter completes; returns its result, or null if it failed without one. */
    private LeaseResponse observe(Waiter target, LongSupplier nowMillis) {
        while (!target.done) {
            awaitMillis(1000L);
        }
        return target.result;
    }

    private Waiter findByRequestId(String requestId) {
        for (Waiter w : waiters) {
            if (w.requestId.equals(requestId)) {
                return w;
            }
        }
        return null;
    }

    private void awaitMillis(long millis) {
        try {
            changed.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new LeaseException(LeaseErrorCode.TEMPORARILY_UNAVAILABLE,
                    "interrupted while waiting for the lease", e);
        }
    }
}

/**
 *   Copyright (c) Rich Hickey. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file epl-v10.html at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 * 	 the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

package clojure.lang;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings({"SynchronizeOnNonFinalField"})
public class LockingTransaction {

    // Time constants
    public static final int RETRY_LIMIT = 10000;
    public static final int LOCK_WAIT_MSECS = 100;
    public static final long BARGE_WAIT_NANOS = 10 * 1000000; // 10 millis

    // Transaction states
    static final int RUNNING = 0;
    static final int COMMITTING = 1;
    static final int RETRY = 2;
    static final int KILLED = 3;
    static final int COMMITTED = 4;


    // Retry transaction (on conflict)
    static class RetryEx extends Error {
    }

    // Transaction has stopped (due to barge)
    static class StoppedEx extends Error {
    }

    // Transaction has been aborted
    static class AbortException extends Exception {
    }


    // Last time point consumed by a transaction.
    // Transactions will consume a point for init, for each retry, and on commit
    // if writing. This defines a total order on transaction.
    final static AtomicLong lastPoint = new AtomicLong();


    // Info for a transaction
    public static class Info {
        // Transaction state (RUNNING, COMMITTING, RETRY...)
        final AtomicInteger status;
        // Time point at which transaction first started.
        final long startPoint;
        // Latch, starts at 1 and counts down when transaction stops
        // (successfully or not). Await on this to wait until a transaction has
        // succeeded.
        final CountDownLatch latch;

        public Info(int status, long startPoint) {
            this.status = new AtomicInteger(status);
            this.startPoint = startPoint;
            this.latch = new CountDownLatch(1);
        }

        public boolean running() {
            int s = status.get();
            return s == RUNNING || s == COMMITTING;
        }
    }


    // Transaction info. Can be read by other transactions.
    Info info;
    // Time point at which transaction was first started.
    long startPoint;
    // Time at which transaction first started.
    long startTime;
    // Time point at which current attempt of transaction started.
    long readPoint;
    // Futures created in transaction.
    // Access should be protected using synchronized.
    final Set<TransactionalFuture> futures = Collections.synchronizedSet(
            new HashSet<TransactionalFuture>());


    // Get number of futures, synchronized.
    int numberOfFutures() {
        synchronized (futures) {
            return futures.size();
        }
    }

    // Indicate transaction as having stopped (with certain state).
    // OK to call twice (idempotent).
    void stop(int status) {
        if (info != null) {
            synchronized (info) {
                info.status.set(status);
                // Notify other transactions that are waiting for this one to
                // finish (using blockAndBail).
                info.latch.countDown();
            }
            info = null;
            // From now on, isRunning returns false and all operations on refs
            // (in TransactionalFuture) will throw StoppedEx
        }
        int n_stopped = 0;
        while (n_stopped != numberOfFutures()) {
            Set<TransactionalFuture> fs;
            synchronized (futures) {
                fs = new HashSet<TransactionalFuture>(futures);
            }
            for (TransactionalFuture f_ : fs) {
                f_.stop(status);
            }
            for (TransactionalFuture f_ : fs) {
                try {
                    f_.get();
                    // Should stop 'soon' with ExecutionException wrapping
                    // StoppedEx
                } catch (Exception e) {
                }
            }
            n_stopped = fs.size();
        }
        // If in the mean time new futures were created, stop them as well.
        // No race conditions because 1) get and stop are idempotent; 2) futures
        // only grows, never shrinks; 3) after all gets have returned, futures
        // won't change anymore.
        synchronized (futures) {
            futures.clear();
        }
    }

    // Is this transaction running?
    boolean isRunning() {
        return info != null && info.running();
    }


    // Try to "barge" the other transaction: if this transaction is older, and
    // we've been waiting for at least BARGE_WAIT_NANOS (10 ms), kill the other
    // one.
    boolean barge(LockingTransaction.Info other) {
        boolean barged = false;
        // if this transaction is older, try to abort the other
        if (other != null && bargeTimeElapsed() &&
                startPoint < other.startPoint) {
            barged = other.status.compareAndSet(RUNNING, KILLED);
            if (barged)
                other.latch.countDown();
        }
        return barged;
    }

    // Has enough time elapsed to try to barge?
    private boolean bargeTimeElapsed() {
        return System.nanoTime() - startTime > BARGE_WAIT_NANOS;
    }

    // Block and bail: stop this transaction, wait until other one has finished,
    // then retry.
    Object blockAndBail(LockingTransaction.Info other) {
        stop(RETRY);
        try {
            other.latch.await(LOCK_WAIT_MSECS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            // ignore, retry immediately
        }
        throw new RetryEx();
    }

    // Kill this transaction.
    void abort() throws AbortException {
        stop(KILLED);
        throw new AbortException();
    }


    // Run fn in a transaction.
    // If we're already in a transaction, use that one, else creates one.
    static public Object runInTransaction(Callable fn) throws Exception {
        TransactionalFuture f = TransactionalFuture.getCurrent();
        if (f == null) { // No transaction running: create one
            LockingTransaction t = new LockingTransaction();
            return t.run(fn);
        } else { // Transaction exists
            if (f.tx.info != null) { // Transaction in transaction: simply call fn
                return fn.call();
            } else { // XXX I'm not sure when this happens?
                // XXX This might actually be incorrect: what if a transaction
                // is stopped (through barging) right before an inner dosync
                // gets called?
                return f.tx.run(fn);
            }
        }
    }

    // Run fn in transaction.
    Object run(Callable fn) throws Exception {
        boolean committed = false;
        Object result = null;

        for (int i = 0; !committed && i < RETRY_LIMIT; i++) {
            readPoint = lastPoint.incrementAndGet();
            if (i == 0) {
                startPoint = readPoint;
                startTime = System.nanoTime();
            }
            info = new Info(RUNNING, startPoint);

            TransactionalFuture f_main = null;
            boolean finished = false;
            try {
                f_main = new TransactionalFuture(this, null, fn);
                result = f_main.callAndWait();
                finished = true;
            } catch (ExecutionException ex) {
                // exception in embedded future => retry
                // XXX should we check whether the cause was a StoppedEx or
                // RetryEx? And what if the cause was another ExecutionException
                // wrapping one of these?
            } catch (StoppedEx ex) {
                // eat this, finished will stay false, and we'll retry
            } catch (RetryEx ex) {
                // eat this, finished will stay false, and we'll retry
            } finally {
                if (!finished) {
                    stop(RETRY);
                } else {
                    committed = f_main.commit(this);
                }
            }
        }
        if (!committed)
            throw Util.runtimeException("Transaction failed after reaching retry limit");
        return result;
    }

}

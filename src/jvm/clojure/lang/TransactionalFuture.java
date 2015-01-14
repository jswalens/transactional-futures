/**
 *   Copyright (c). All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file epl-v10.html at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 * 	 the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

package clojure.lang;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class TransactionalFuture {

    // Commute function
    static class CFn {
        final IFn fn;
        final ISeq args;

        public CFn(IFn fn, ISeq args) {
            this.fn = fn;
            this.args = args;
        }
    }


    // Future running in current thread (can be null)
    final static ThreadLocal<TransactionalFuture> future = new ThreadLocal<TransactionalFuture>();


    // Transaction for this future
    final LockingTransaction tx;

    // In transaction values of refs (both read and written)
    final Map<Ref, Object> vals = new HashMap<Ref, Object>();
    // Refs set in transaction. (Their value is in vals.)
    final Set<Ref> sets = new HashSet<Ref>();
    // Refs commuted, and list of commute functions.
    final Map<Ref, ArrayList<CFn>> commutes = new TreeMap<Ref, ArrayList<CFn>>();
    // Ensured refs. All hold readLock.
    final Set<Ref> ensures = new HashSet<Ref>();
    // Agent sends.
    final List<Agent.Action> actions = new ArrayList<Agent.Action>();

    // Is transaction running?
    // Used to stop this future from elsewhere (e.g. for barge).
    final AtomicBoolean running = new AtomicBoolean(false);


    TransactionalFuture(LockingTransaction tx) {
        this.tx = tx;
        running.set(true);
    }


    // Is this thread in a future?
    static public boolean isActive() {
        return getCurrent() != null;
    }

    // Get this thread's future (possibly null).
    static TransactionalFuture getCurrent() {
        return future.get();
    }

    // Get this thread's future. Throws exception if no future/transaction is
    // running.
    static TransactionalFuture getEx() {
        TransactionalFuture f = future.get();
        if (f == null) {
            // no future should => no transaction
            assert LockingTransaction.getCurrent() == null;
            throw new IllegalStateException("No transaction running");
        }
        return f;
    }


    // Run fn in a future, in transaction tx.
    // If we're already in a transaction, fails.
    // Can throw RetryEx or StoppedEx.
    static public Object runInFuture(LockingTransaction tx, Callable fn)
    throws Exception, LockingTransaction.RetryEx, LockingTransaction.StoppedEx {
        TransactionalFuture f = future.get();
        if (f != null)
            throw new IllegalStateException("Already in a future");

        f = new TransactionalFuture(tx);
        future.set(f);
        tx.futures.add(f);
        Object ret = null;
        try {
            ret = fn.call();
        } finally {
            future.remove();
        }
        return ret;
    }


    // Indicate future as having stopped (with certain transaction state).
    // OK to call twice (idempotent).
    void stop(int status) {
        running.set(false);
        // From now on, all operations on refs will throw StoppedEx
        vals.clear();
        sets.clear();
        commutes.clear();
        for (Ref r : ensures) {
            r.unlockRead();
        }
        ensures.clear();
        try {
            if (status == LockingTransaction.COMMITTED) {
                for (Agent.Action action : actions) {
                    Agent.dispatchAction(action);
                    // By now, TransactionFuture.future.get() is null, so
                    // dispatches happen immediately
                }
            }
        } finally {
            actions.clear();
        }
    }

    // Get
    Object doGet(Ref ref) {
        if (!running.get())
            throw new LockingTransaction.StoppedEx();
        if (vals.containsKey(ref))
            return vals.get(ref);
        try {
            ref.lockRead();
            if (ref.tvals == null)
                throw new IllegalStateException(ref.toString() + " is unbound.");
            Ref.TVal ver = ref.tvals;
            do {
                if (ver.point <= tx.readPoint)
                    return ver.val;
            } while ((ver = ver.prior) != ref.tvals);
        } finally {
            ref.unlockRead();
        }
        // No version of val precedes the read point (not enough versions kept)
        ref.faults.incrementAndGet();
        throw new LockingTransaction.RetryEx();
    }

    // Set
    Object doSet(Ref ref, Object val) {
        if (!running.get())
            throw new LockingTransaction.StoppedEx();
        if (commutes.containsKey(ref))
            throw new IllegalStateException("Can't set after commute");
        if (!sets.contains(ref)) {
            sets.add(ref);
            releaseIfEnsured(ref);
            ref.lockWrite(tx);
        }
        vals.put(ref, val);
        return val;
    }

    // Ensure
    void doEnsure(Ref ref) {
        if (!running.get())
            throw new LockingTransaction.StoppedEx();
        if (ensures.contains(ref))
            return;
        ref.lockRead();

        // Someone completed a write after our snapshot => retry
        if (ref.tvals != null && ref.tvals.point > tx.readPoint) {
            ref.unlockRead();
            throw new LockingTransaction.RetryEx();
        }

        LockingTransaction.Info latestWriter = ref.latestWriter;

        // Writer exists (maybe us?)
        if (latestWriter != null && latestWriter.running()) {
            ref.unlockRead();

            if (latestWriter != tx.info) { // Not us, ensure is doomed
                tx.blockAndBail(latestWriter);
            }
        } else {
            ensures.add(ref);
        }
    }

    // Commute
    Object doCommute(Ref ref, IFn fn, ISeq args) {
        if (!running.get())
            throw new LockingTransaction.StoppedEx();
        if (!vals.containsKey(ref)) {
            Object val = null;
            try {
                ref.lockRead();
                val = ref.tvals == null ? null : ref.tvals.val;
            } finally {
                ref.unlockRead();
            }
            vals.put(ref, val);
        }
        ArrayList<CFn> fns = commutes.get(ref);
        if (fns == null)
            commutes.put(ref, fns = new ArrayList<CFn>());
        fns.add(new CFn(fn, args));
        Object ret = fn.applyTo(RT.cons(vals.get(ref), args));
        vals.put(ref, ret);
        return ret;
    }

    void releaseIfEnsured(Ref ref) {
        if (ensures.contains(ref)) {
            ensures.remove(ref);
            ref.unlockRead();
        }
    }

    // Agent send
    void enqueue(Agent.Action action) {
        actions.add(action);
    }


    // Notify watches
    private static class Notify {
        final public Ref ref;
        final public Object oldval;
        final public Object newval;

        Notify(Ref ref, Object oldval, Object newval) {
            this.ref = ref;
            this.oldval = oldval;
            this.newval = newval;
        }
    }

    // Commit
    boolean commit(LockingTransaction tx) {
        assert tx.isActive();

        boolean done = false;
        ArrayList<Ref> locked = new ArrayList<Ref>(); // write locks
        ArrayList<Notify> notify = new ArrayList<Notify>();
        try {
            // If no one has killed us before this point, and make sure they
            // can't from now on. If they have: retry, done stays false.
            if (!tx.info.status.compareAndSet(LockingTransaction.RUNNING,
                    LockingTransaction.COMMITTING)) {
                throw new LockingTransaction.RetryEx();
            }

            // Commutes: write-lock them, re-calculate and put in vals
            for (Map.Entry<Ref, ArrayList<CFn>> e : commutes.entrySet()) {
                Ref ref = e.getKey();
                if (sets.contains(ref)) {
                    // commute and set: no need to re-execute, use latest val
                    continue;
                }

                boolean wasEnsured = ensures.contains(ref);
                // Can't upgrade readLock, so release it
                releaseIfEnsured(ref);
                ref.tryWriteLock();
                locked.add(ref);
                if (wasEnsured && ref.tvals != null && ref.tvals.point > tx.readPoint)
                    throw new LockingTransaction.RetryEx();

                LockingTransaction.Info latest = ref.latestWriter;
                if (latest != null && latest != tx.info && latest.running()) {
                    boolean barged = tx.barge(latest);
                    // Try to barge other, if it didn't work retry this tx
                    if (!barged)
                        throw new LockingTransaction.RetryEx();
                }
                Object val = ref.tvals == null ? null : ref.tvals.val;
                vals.put(ref, val);
                for (CFn f : e.getValue()) {
                    vals.put(ref, f.fn.applyTo(RT.cons(vals.get(ref), f.args)));
                }
            }

            // Sets: write-lock them
            for (Ref ref : sets) {
                ref.tryWriteLock();
                locked.add(ref);
            }

            // Validate (if invalid, throws IllegalStateException)
            for (Map.Entry<Ref, Object> e : vals.entrySet()) {
                Ref ref = e.getKey();
                ref.validate(ref.getValidator(), e.getValue());
            }

            // At this point, all values calced, all refs to be written locked,
            // so commit.
            long commitPoint = LockingTransaction.lastPoint.incrementAndGet();
            for (Map.Entry<Ref, Object> e : vals.entrySet()) {
                Ref ref = e.getKey();
                Object oldval = ref.tvals == null ? null : ref.tvals.val;
                Object newval = e.getValue();
                int hcount = ref.histCount();

                if (ref.tvals == null) {
                    ref.tvals = new Ref.TVal(newval, commitPoint);
                } else if ((ref.faults.get() > 0 && hcount < ref.maxHistory)
                        || hcount < ref.minHistory) {
                    ref.tvals = new Ref.TVal(newval, commitPoint, ref.tvals);
                    ref.faults.set(0);
                } else {
                    ref.tvals = ref.tvals.next;
                    ref.tvals.val = newval;
                    ref.tvals.point = commitPoint;
                }
                // Notify refs with watches
                if (ref.getWatches().count() > 0)
                    notify.add(new Notify(ref, oldval, newval));
            }

            // Done
            tx.info.status.set(LockingTransaction.COMMITTED);
            done = true;
        } catch (LockingTransaction.RetryEx ex) {
            // eat this, done will stay false
        } finally {
            // Unlock
            for (int k = locked.size() - 1; k >= 0; --k) {
                locked.get(k).unlockWrite();
            }
            locked.clear();
            // Clear properties of tx and its futures
            tx.stop(done ? LockingTransaction.COMMITTED : LockingTransaction.RETRY);
            // Send notifications
            try {
                if (done) {
                    for (Notify n : notify) {
                        n.ref.notifyWatches(n.oldval, n.newval);
                    }
                }
            } finally {
                notify.clear();
            }
        }
        return done;
    }

}

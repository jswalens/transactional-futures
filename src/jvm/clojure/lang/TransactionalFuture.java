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
import java.util.concurrent.*;

public class TransactionalFuture implements Callable, Future {

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

    // Java Future executing this future.
    // null if executing in main thread.
    Future fut = null;

    // Function executed in this future
    final Callable fn;
    // Result of future (return value of fn)
    Object result;

    static class Vals<K, V> {
        final Map<K, V> vals = new HashMap<K, V>();
        final Vals<K, V> prev;

        Vals() { this.prev = null; }
        Vals(Vals<K, V> prev) { this.prev = prev; }

        public V get(K key) {
            V val = vals.get(key);
            if (val == null && prev != null)
                return prev.get(key);
            return val;
        }

        public V put(K key, V value) {
            return vals.put(key, value);
        }

        public void putAll(Map<? extends K, ? extends V> m) {
            vals.putAll(m);
        }

        public boolean isEmpty() {
            return vals.isEmpty();
        }

        public void clear() { // XXX
            this.vals.clear();
            //this.prev = null; // XXX is this ok?
        }
    }

    // In transaction values of refs (written by set or commute)
    // The keys of this map and its prev's = union of sets and commutes.
    Vals<Ref, Object> vals;
    // Refs set (not commuted) in this future. (Their value is in vals.)
    final Set<Ref> sets = new HashSet<Ref>();
    // Snapshot: in transaction values of refs at the moment of
    // creation of this future (set in any ancestor)
    final Vals<Ref, Object> snapshot;
    // Refs commuted, and list of commute functions
    final Map<Ref, ArrayList<CFn>> commutes = new TreeMap<Ref, ArrayList<CFn>>();
    // Ensured refs. All hold readLock.
    final Set<Ref> ensures = new HashSet<Ref>();
    // Agent sends
    final List<Agent.Action> actions = new ArrayList<Agent.Action>();
    // Futures, merged into this one
    final Set<TransactionalFuture> merged = new HashSet<TransactionalFuture>();


    TransactionalFuture(LockingTransaction tx, TransactionalFuture parent,
        Callable fn) {
        this.tx = tx;
        this.fn = fn;

        // Initialize vals to parent vals
        if (parent != null) {
            if (!parent.vals.isEmpty()) {
                snapshot = parent.vals;
                vals = new Vals<Ref, Object>(parent.vals);
                parent.vals = new Vals<Ref, Object>(parent.vals);
            } else {
                // Optimization: if parent has not set anything, this can point
                // straight to the parent's ancestor, and parent can "re-use"
                // his vals. This way we avoid creating empty vals.
                snapshot = parent.vals.prev;
                vals = new Vals<Ref, Object>(parent.vals.prev);
            }
        } else {
            snapshot = null;
            vals = new Vals<Ref, Object>();
        }

        synchronized (tx.futures) {
            tx.futures.add(this);
        }
    }


    // Is this thread in a transactional future?
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
            throw new IllegalStateException("No transaction running");
        }
        return f;
    }


    // Execute future (in this thread).
    public Object call() throws Exception {
        if(!tx.isRunning())
            throw new LockingTransaction.StoppedEx();

        TransactionalFuture f = future.get();
        if (f != null)
            throw new IllegalStateException("Already in a future");

        try {
            future.set(this);
            result = fn.call();
        } finally {
            future.remove();
        }
        return result;
    }

    // Execute future (in this thread), and wait for all sub-futures to finish.
    // This will throw an ExecutionException if an inner future threw an
    // exception (e.g. StoppedEx or RetryEx).
    public Object callAndWait() throws Exception {
        if(!tx.isRunning())
            throw new LockingTransaction.StoppedEx();

        TransactionalFuture f = future.get();
        if (f != null)
            throw new IllegalStateException("Already in a future");

        try {
            future.set(this);
            result = fn.call();

            // Wait for all futures to finish
            int n_stopped = 0;
            while (n_stopped != tx.numberOfFutures()) {
                Set<TransactionalFuture> fs;
                synchronized (tx.futures) {
                    fs = new HashSet<TransactionalFuture>(tx.futures);
                }
                for (TransactionalFuture f_ : fs) {
                    if (f_ != this) // Don't merge into self
                        f_.get();
                }
                n_stopped = fs.size();
            }
            // If in the mean time new futures were created, wait for them
            // as well. No race condition because number of futures won't
            // change for sure after last get, and only increases.
        } finally {
            future.remove();
        }
        return result;
    }

    // Execute future in another thread.
    public void spawn() {
        fut = Agent.soloExecutor.submit(this);
    }

    // Spawn future: outside transaction regular future, in transactional a
    // transactional future.
    static public Future spawnFuture(Callable fn) {
        TransactionalFuture current = TransactionalFuture.getCurrent();
        if (current == null) { // outside transaction
            return Agent.soloExecutor.submit(fn);
        } else { // inside transaction
            if (!current.tx.isRunning())
                throw new LockingTransaction.StoppedEx();
            TransactionalFuture f = new TransactionalFuture(current.tx, current,
                fn);
            f.spawn();
            return f;
        }
    }


    // Attempts to cancel execution of this task.
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (fut != null)
            return fut.cancel(mayInterruptIfRunning);
        else
            return false;
    }

    // Waits if necessary for the computation to complete, and then retrieves
    // its result.
    // Should only be called in another future.
    // Throws ExecutionException if an exception occurred in the future. (This
    // might be a RetryEx or a StoppedEx!)
    public Object get() throws ExecutionException, InterruptedException {
        // Note: in future_a, we call future_b.get()
        // => this = future_b; current = future_a
        TransactionalFuture current = TransactionalFuture.getEx();

        // Wait for other thread to finish
        if (fut != null)
            fut.get(); // sets result
        // else: result set by call() directly

        // Merge into current
        current.merge(this);

        return result;
    }

    // Waits if necessary for at most the given time for the computation to
    // complete, and then retrieves its result, if available.
    public Object get(long timeout, TimeUnit unit) throws InterruptedException,
    ExecutionException, TimeoutException {
        if (fut != null)
            return fut.get(timeout, unit);
        else
            return result;
    }

    // Returns true if this task was cancelled before it completed normally.
    public boolean isCancelled() {
        if (fut != null)
            return fut.isCancelled();
        else
            return false;
    }

    // Returns true if this task completed.
    public boolean isDone() {
        if (fut != null)
            return fut.isCancelled();
        else
            return result != null; // XXX could also mean the result was actually null?
    }


    // Indicate future as having stopped (with certain transaction state).
    // OK to call twice (idempotent).
    void stop(int status) {
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
        if (!tx.isRunning())
            throw new LockingTransaction.StoppedEx();
        Object val = vals.get(ref);
        if (val == null)
            return requireBeforeTransaction(ref);
        return val;
    }

    // Get the version of ref before the transaction started, or null if the
    // version doesn't exist anymore.
    Ref.TVal getBeforeTransaction(Ref ref) {
        try {
            ref.lockRead();
            if (ref.tvals == null)
                throw new IllegalStateException(ref.toString() + " is unbound.");
            Ref.TVal ver = ref.tvals;
            do {
                if (ver.point <= tx.readPoint)
                    return ver;
            } while ((ver = ver.prior) != ref.tvals);
        } finally {
            ref.unlockRead();
        }
        return null;
    }

    // Returns the value of ref before the transaction started, or throws
    // RetryEx if no recent version could be found.
    Object requireBeforeTransaction(Ref ref) {
        Ref.TVal ver = getBeforeTransaction(ref);
        if (ver != null) {
            return ver.val;
        } else {
            // No version of val precedes the read point (not enough versions
            // kept)
            ref.faults.incrementAndGet();
            throw new LockingTransaction.RetryEx();
        }
    }

    // Set
    Object doSet(Ref ref, Object val) {
        if (!tx.isRunning())
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
        if (!tx.isRunning())
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
        if (!tx.isRunning())
            throw new LockingTransaction.StoppedEx();
        Object val = vals.get(ref);
        if (val == null) {
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
        Object ret = fn.applyTo(RT.cons(val, args));
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

    // Merge other future into current one
    void merge(TransactionalFuture child) {
        if (merged.contains(child))
            return;

        // vals: add in-transaction-value of refs SET in child to parent; refs
        // READ in the child are ignored. Call custom resolve function if
        // present and ref was set in parent since creation.
        for (Ref r : child.sets) {
            // Current value in child: always present in child.vals, because r
            // is in child.sets
            Object v_child = child.vals.get(r);
            // Current value in parent: first look in vals, if it isn't there
            // look up the value before the transaction
            Object v_parent = vals.get(r);
            if (v_parent == null)
                v_parent = requireBeforeTransaction(r);
            // Get original value, i.e. value when child was created: first look
            // in snapshot, if it isn't in snapshot look for value before
            // transaction
            Object v_original = null;
            if (child.snapshot != null)
                v_original = child.snapshot.get(r);
            if (v_original == null)
                v_original = requireBeforeTransaction(r);

            if (v_parent == v_original) { // no conflict, just take over value
                vals.put(r, v_child);
            } else { // conflict
                if (r.getResolve() != null) {
                    IFn resolve = r.getResolve();
                    Object v = resolve.invoke(v_original, v_parent, v_child);
                    vals.put(r, v);
                } else {
                    vals.put(r, v_child);
                }
            }
        }
        // sets: add sets of child to parent
        sets.addAll(child.sets);
        // commutes: add commutes of child to parent
        // order doesn't matter because they're commutative
        for (Map.Entry<Ref, ArrayList<CFn>> c : child.commutes.entrySet()) {
            ArrayList<CFn> fns = commutes.get(c.getKey());
            if (fns == null) {
                commutes.put(c.getKey(), fns = new ArrayList<CFn>());
            }
            fns.addAll(c.getValue());
        }
        // ensures: add ensures of child to parent
        ensures.addAll(child.ensures);
        // actions: add actions of child to parent
        // They are added AFTER the ones of the current future, in the order
        // they were in in the child
        actions.addAll(child.actions);
        // merged: add futures merged into child to futures merged into parent
        merged.addAll(child.merged);

        merged.add(child);
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
                sets.add(ref);
            }

            // Sets: write-lock them
            for (Ref ref : sets) {
                ref.tryWriteLock();
                locked.add(ref);
            }

            // Validate (if invalid, throws IllegalStateException)
            for (Ref ref : sets) {
                ref.validate(ref.getValidator(), vals.get(ref));
            }

            // At this point, all values calced, all refs to be written locked,
            // so commit.
            long commitPoint = LockingTransaction.lastPoint.incrementAndGet();
            for (Ref ref : sets) {
                Object oldval = ref.tvals == null ? null : ref.tvals.val;
                Object newval = vals.get(ref);
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

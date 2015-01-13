/**
 *   Copyright (c) Rich Hickey. All rights reserved.
 *   The use and distribution terms for this software are covered by the
 *   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
 *   which can be found in the file epl-v10.html at the root of this distribution.
 *   By using this software in any fashion, you are agreeing to be bound by
 * 	 the terms of this license.
 *   You must not remove this notice, or any other, from this software.
 **/

/* rich Jul 26, 2007 */

package clojure.lang;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings({"SynchronizeOnNonFinalField"})
public class LockingTransaction {

	public static final int RETRY_LIMIT = 10000;
	public static final int LOCK_WAIT_MSECS = 100; // XXX also moved to Ref, maybe not needed anymore here?
	public static final long BARGE_WAIT_NANOS = 10 * 1000000; // 10 millis
	//public static int COMMUTE_RETRY_LIMIT = 10;

	static final int RUNNING = 0;
	static final int COMMITTING = 1;
	static final int RETRY = 2;
	static final int KILLED = 3;
	static final int COMMITTED = 4;

	final static ThreadLocal<LockingTransaction> transaction = new ThreadLocal<LockingTransaction>();


	static class RetryEx extends Error {
	}

	static final RetryEx RETRY_EX = new RetryEx();

	static class AbortException extends Exception {
	}

	public static class Info {
		final AtomicInteger status;
		final long startPoint;
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


	// Total order on transactions.
	// Transactions will consume a point for init, for each retry, and on commit
	// if writing.
	final private static AtomicLong lastPoint = new AtomicLong();

	void acquireReadPoint() {
		readPoint = lastPoint.incrementAndGet();
	}

	long acquireCommitPoint() {
		return lastPoint.incrementAndGet();
	}


	Info info;
	long readPoint;
	long startPoint;
	long startTime;
	List<TransactionalFuture> futures = new ArrayList<TransactionalFuture>();


	void stop(int status) {
		if (info != null) {
			synchronized (info) {
				info.status.set(status);
				// Notify other transactions that are waiting for this one to
				// finish (using blockAndBail).
				info.latch.countDown();
			}
			info = null;
			for (TransactionalFuture f : futures)
				f.stop();
			futures.clear();
		}
	}


	// Try to "barge" the given transaction (if this one is older, will kill
	// the other one).
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

	private boolean bargeTimeElapsed() {
		return System.nanoTime() - startTime > BARGE_WAIT_NANOS;
	}

	// Block and bail: stop this transaction, wait until other one has finished,
	// then retry.
	Object blockAndBail(LockingTransaction.Info other) {
		stop(RETRY);
		if (other != null) {
			try {
				other.latch.await(LOCK_WAIT_MSECS, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				// ignore, retry immediately
			}
		}
		throw new RetryEx();
	}

	// Kill this transaction.
	void abort() throws AbortException {
		stop(KILLED);
		throw new AbortException();
	}


	// Are we currently in a transaction?
	static public boolean isActive() {
		return getCurrent() != null;
	}

	static LockingTransaction getCurrent() {
		LockingTransaction t = transaction.get();
		if (t == null || t.info == null)
			return null;
		return t;
	}

	// getCurrent, but throw exception if no transaction is running.
	static LockingTransaction getEx() {
		LockingTransaction t = transaction.get();
		if (t == null || t.info == null)
			throw new IllegalStateException("No transaction running");
		return t;
	}


	static public Object runInTransaction(Callable fn) throws Exception {
		LockingTransaction t = transaction.get();
		Object ret;
		if (t == null) { // No transaction running: create one
			t = new LockingTransaction();
			transaction.set(t);
			try {
				ret = t.run(fn);
			} finally {
				transaction.remove();
			}
		} else { // Transaction exists
			if (t.info != null) { // Transaction in transaction: simply call fn
				ret = fn.call();
			} else {
				ret = t.run(fn);
			}
		}

		return ret;
	}

	Object run(Callable fn) throws Exception {
		boolean committed = false;
		Object ret = null;

		assert transaction.get() == this;

		for (int i = 0; !committed && i < RETRY_LIMIT; i++) {
			boolean finished = false;

			acquireReadPoint();
			if (i == 0) {
				startPoint = readPoint;
				startTime = System.nanoTime();
			}
			info = new Info(RUNNING, startPoint);
			TransactionalFuture f = new TransactionalFuture(this);
			futures.add(f);
			assert TransactionalFuture.future.get() == f; // XXX
			assert TransactionalFuture.future.get() == futures.get(0); // XXX
			assert futures.size() == 1;

			try {
				ret = fn.call();
				assert futures.size() == 1;
				finished = true;
			} catch (TransactionalFuture.StoppedEx ex) { // XXX can this happen?
				// eat this, success will stay false
			} catch (RetryEx ex) {
				// eat this, success will stay false
			}
			TransactionalFuture.future.remove();
			if (!finished) {
				stop(RETRY); // XXX maybe we're calling this twice (if barged)
			} else {
				assert futures.size() == 1;
				committed = futures.get(0).commit(this);
			}
		}
		if (!committed)
			throw Util.runtimeException("Transaction failed after reaching retry limit");
		return ret;
	}

}

(ns clojure.test-clojure.transactional-promises
  (:use clojure.test))

(deftest test-atomicity []
  "The effects of a deliver should only be visible together with the effects of
  its containing transaction."
  (let [x (ref 0)
        y (ref 0)
        p (promise)
        a1 (atom 0)
        a2 (atom 0)
        f1 (future
             (dosync ; transaction 1: should fail, but preferably only once
               (swap! a1 inc) ; count attempts
               (is (or (= @x 0) (= @x 1)))
               (is (or (= @y 0) (= @y 1)))
               (is (= @p 5)) ; waits
               ; if previous deref succeeds, transaction 1 has committed and its
               ; effects should be visible
               (is (= @x 1))
               (is (= @y 1)))
             (is (= @x 1))
             (is (= @y 1))
             (is (= @p 5))
             (is (> @a1 1))) ; actually not sure, depends on order of futures
        f2 (future
             (dosync ; transaction 2: should never fail
               (swap! a2 inc) ; count attempts
               (is (= @x 0))
               (is (= @y 0))
               (ref-set x 1)
               (is (= @x 1))
               (Thread/sleep 50) ; transaction 1 will have to wait for this one
               (deliver p 5)
               (is (= @p 5))
               (ref-set y 1)
               (is (= @y 1)))
             (is (= @x 1))
             (is (= @y 1))
             (is (= @p 5))
             (is (= @a2 1)))]
    @f1
    @f2))

(deftest test-double-delivery []
  "Deliveries should be 'undone' when a transaction is aborted."
  (let [x (ref 0)
        y (ref 0)
        p (promise)
        a1 (atom 0)
        a2 (atom 0)
        f1 (future
             (dosync
               (swap! a1 inc)
               (Thread/sleep 50)
               (deliver p @a1) ; first attempt delivers 1; second delivers 2
               (when (= @a1 1)
                 @(future (dosync (Thread/sleep 20) (alter x inc))); force retry on first attempt
                 (ensure x))))
        f2 (future
             (is (= @p 2)))] ; first attempt should not succeed
    @f1
    @f2))

(deftest test-regular-promise []
  "Promises not in a transaction should work as before."
  (let [p (promise)
        f1 (future
             (is (= @p 5)))
        f2 (future
             (is (= @(deliver p 5) 5))
             (is (= (deliver p 7) nil))
             (is (= @p 5)))]
    @f1
    @f2))

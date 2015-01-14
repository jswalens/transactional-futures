;   Copyright (c). All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns clojure.test-clojure.transactional-futures
  (:use clojure.test)
  (:import (java.util.concurrent Executors)))

; === SIMPLE ===

(deftest seq-alter
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (alter r1 inc)
        (is (= :abc (deref (future (alter r1 inc) :abc))))
        (alter r1 inc))
      (is (= 3 (deref r1))))))

(deftest concurrent-alter
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (alter r1 inc)
        (let [f (future (dotimes [j 1010] (alter r1 inc)) :abc)]
          (dotimes [j 1020] (alter r1 inc))
          (is (= :abc (deref f)))))
      ; merges and overwrites -> 1020 above ignored
      (is (= 1011 (deref r1))))))

(deftest concurrent-alter-1
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (alter r1 inc)
        (let [f (future (alter r1 + 20) :abc)]
          (Thread/sleep 10)
          (alter r1 + 10)
          (is (= :abc (deref f)))))
      ; discards 10 from above
      (is (= 21 (deref r1))))))

(deftest concurrent-alter-2
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (alter r1 inc)
        (let [f (future (Thread/sleep 10) (alter r1 + 20))]
          (alter r1 + 10)
          (deref f)))
      ; discards 10 from above
      (is (= 21 (deref r1))))))

(comment
(deftest concurrent-commute-after-alter
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (alter r1 inc)
        (let [f (future (dotimes [j 1010] (commute r1 inc)))]
          (dotimes [j 1020] (commute r1 inc))
          (deref f)))
      ; XXX commute and alter are different! alter can be overwritten,
      ; commute won't. Isn't this confusing?
      (is (= 2031 (deref r1))))))
; TODO: this doesn't work because it's a commute after an alter and that's
; different from just a commute.
; (In this case, during the commit, the last in-tx-value is written; in
; the case of only commutes the commutes are re-executed.)
  )

(deftest concurrent-commute
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (commute r1 inc)
        (let [f (future (dotimes [j 1010] (commute r1 inc)))]
          (dotimes [j 1020] (commute r1 inc))
          (deref f)))
      ; XXX same as above
      (is (= 2031 (deref r1))))))

(deftest concurrent-commute-1
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (commute r1 inc)
        (let [f (future (commute r1 + 20))]
          (Thread/sleep 10)
          (commute r1 + 10)
          (deref f)))
      (is (= 31 (deref r1))))))

(deftest concurrent-commute-2
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (commute r1 inc)
        (let [f (future (Thread/sleep 10) (commute r1 + 20))]
          (commute r1 + 10)
          (deref f)))
      (is (= 31 (deref r1))))))

(deftest concurrent-set
  (dotimes [i 100]
    (let [r1 (ref 0)]
      (dosync
        (let [f (future (ref-set r1 (+ (deref r1) 10)))]
          (ref-set r1 (+ (deref r1) 20)) ; this deref should never read the value set in the future
          (deref f)))
      (is (= 10 (deref r1))))))

(deftest concurrent-set-no-conflict
  (dotimes [i 50]
    (let [r1 (ref 0)
          r2 (ref 1)]
      (dosync
        (let [f (future (ref-set r1 (+ (deref r1) 10)))]
          (ref-set r2 (+ (deref r2) 20))
          (is (= 10 (deref f))))) ; no conflicts here
      (is (= 10 (deref r1)))
      (is (= 21 (deref r2))))))

(deftest concurrent-set-conflict-nondet
  (dotimes [i 50]
    (let [r1 (ref 0)
          r2 (ref 1)]
      (dosync
        (let [f (future (ref-set r1 (+ (deref r2) 10)))]
          (ref-set r2 (+ (deref r1) 20))
          (deref f)))
      ; in previous versions there was non-determinism,
      ; now deterministic:
      (is (= 11 (deref r1)))
      (is (= 20 (deref r2))))))

(deftest no-deref
  (dotimes [i 50]
    (let [r1 (ref 0)]
      (dosync
        (future (Thread/sleep 10) (ref-set r1 1)))
      (is (= 1 (deref r1)))))) ; no conflict

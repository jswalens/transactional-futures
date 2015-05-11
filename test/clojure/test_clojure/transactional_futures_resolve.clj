;   Copyright (c). All rights reserved.
;   The use and distribution terms for this software are covered by the
;   Eclipse Public License 1.0 (http://opensource.org/licenses/eclipse-1.0.php)
;   which can be found in the file epl-v10.html at the root of this distribution.
;   By using this software in any fashion, you are agreeing to be bound by
;   the terms of this license.
;   You must not remove this notice, or any other, from this software.


(ns clojure.test-clojure.transactional-futures-resolve
  (:use clojure.test))

; TODO:
; 1. Tests with different structures, mirroring transactional_futures.clj
; 2. Tests with different resolve functions

; === DIFFERENT RESOLVE FUNCTIONS ===

(deftest sum
  (dotimes [i 50]
    (let [r1 (ref 0 :resolve (fn [o p c] (+ p (- c o))))]
      (dosync
        (alter r1 inc)
        (let [f (future (dotimes [j 100] (alter r1 inc)) (is (= 101 @r1)) :abc)]
          (dotimes [j 200] (alter r1 inc))
          (is (= 201 @r1))
          (is (= :abc @f))
          ; (+ 201 (- 101 1)) = (+ 301)
          (is (= 301 @r1))))
      (is (= 301 @r1)))))
